"""
tunehud_gateway/main.py
TuneHUD Gateway — multi-controller, serves dashboard, Python 3.7+

Usage:
  python main.py --config configs/demo.yaml
  python main.py --config configs/multi.yaml --verbose
"""
from __future__ import annotations
import asyncio
import csv
import json
import logging
import argparse
import os
import sys
import time
import mimetypes
from datetime import datetime
from pathlib import Path

import websockets
try:
    # websockets >= 14 (asyncio-based)
    from websockets.asyncio.server import serve as ws_serve, ServerConnection
    from websockets.http11 import Request, Response
    from websockets.datastructures import Headers
    WS_V2 = True
except ImportError:
    # websockets 9-11 (legacy)
    ws_serve = websockets.serve
    WS_V2 = False

try:
    from websockets.legacy.server import WebSocketServerProtocol
except ImportError:
    try:
        from websockets.server import WebSocketServerProtocol
    except ImportError:
        WebSocketServerProtocol = object

sys.path.insert(0, os.path.dirname(__file__))

from config import load_config
from plugins import get_plugin

# Camera streamer — optional, disabled if picamera2 not installed
try:
    from camera import CameraStreamer
    HAS_CAMERA = True
except ImportError:
    HAS_CAMERA = False

log = logging.getLogger('tunehud.gateway')

DASHBOARD_FILE = os.path.join(os.path.dirname(__file__), 'tunehud_dashboard.html')


# ── Session logger ─────────────────────────────────────────────────────────

class SessionLogger:
    def __init__(self, log_dir):
        self._log_dir = Path(log_dir)
        self._log_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime('%Y%m%d_%H%M%S')
        self._stream_path = self._log_dir / ('session_{}.csv'.format(ts))
        self._writes_path = self._log_dir / ('session_{}_writes.csv'.format(ts))
        self._stream_file = None
        self._writes_file = None
        self._stream_writer = None
        self._writes_writer = None
        self._stream_header_written = False
        self._stream_keys = []
        log.info('Session log: {}'.format(self._stream_path))

    def open(self):
        self._stream_file = open(str(self._stream_path), 'w', newline='')
        self._writes_file = open(str(self._writes_path), 'w', newline='')
        self._writes_writer = csv.writer(self._writes_file)
        self._writes_writer.writerow(['t', 'controller', 'name', 'prev_value', 'new_value', 'client'])
        self._writes_file.flush()

    def init_stream_header(self, keys):
        if self._stream_header_written or not self._stream_file:
            return
        self._stream_keys = keys
        self._stream_writer = csv.writer(self._stream_file)
        self._stream_writer.writerow(['t'] + keys)
        self._stream_file.flush()
        self._stream_header_written = True

    def log_stream(self, t, data):
        if not self._stream_writer:
            return
        self._stream_writer.writerow(
            ['{:.3f}'.format(t)] + [str(data.get(k, '')) for k in self._stream_keys])
        self._stream_file.flush()

    def log_write(self, t, controller, name, prev, new, client):
        if not self._writes_writer:
            return
        self._writes_writer.writerow(['{:.3f}'.format(t), controller, name, prev, new, client])
        self._writes_file.flush()

    def close(self):
        if self._stream_file:
            self._stream_file.close()
        if self._writes_file:
            self._writes_file.close()


# ── Gateway ────────────────────────────────────────────────────────────────

class TuneHUDGateway:
    def __init__(self, config, log_dir=None):
        self.cfg = config
        self._clients = set()
        self._device_clients = {}
        self._camera = CameraStreamer(width=1920, height=1080, fps=30) if HAS_CAMERA else None
        self._stream_hz = min(config.stream.default_hz, config.stream.max_hz)
        self._logger = SessionLogger(log_dir) if log_dir else None
        self._last_values = {}   # controller.param -> value

        # One plugin per controller
        self._plugins = {}
        self._manifests = {}
        for c in config.controllers:
            plugin_cfg = dict(list(c.options.items()) + [
                ('name', c.name),
                ('device', c.name),
                ('map', c.map),
            ])
            self._plugins[c.name] = get_plugin(c.type, plugin_cfg)

    def _combined_manifest(self):
        """Build combined manifest from all connected controllers."""
        controllers = []
        for name, manifest in self._manifests.items():
            cfg = next((c for c in self.cfg.controllers if c.name == name), None)
            controllers.append({
                'name':    name,
                'device':  manifest.get('device', name),
                'transport': manifest.get('transport', ''),
                'params':  manifest.get('params', []),
                'gauges':  cfg.gauges if cfg else [],
            })
        return {
            'type':        'manifest_resp',
            'title':       self.cfg.title,
            'controllers': controllers,
        }

    def _remote_addr(self, ws):
        try:
            return ws.remote_address
        except AttributeError:
            try:
                return ws.request.remote_address
            except Exception:
                return 'unknown'

    async def _register(self, ws):
        self._clients.add(ws)
        log.info('Client connected: {} ({} total)'.format(
            self._remote_addr(ws), len(self._clients)))
        if self._manifests:
            await ws.send(json.dumps(self._combined_manifest()))

    async def _unregister(self, ws):
        self._clients.discard(ws)
        # If this was a registered device, remove it
        if id(ws) in self._device_clients:
            dev = self._device_clients.pop(id(ws))
            ctrl_name = dev['name']
            self._manifests.pop(ctrl_name, None)
            log.info('Device disconnected: {}'.format(ctrl_name))
            # Notify dashboard clients
            manifest_msg = json.dumps(self._combined_manifest())
            for client in list(self._clients):
                try:
                    await client.send(manifest_msg)
                except Exception:
                    pass
        log.info('Client disconnected ({} remaining)'.format(len(self._clients)))

    async def _handle_message(self, ws, raw):
        try:
            msg = json.loads(raw)
        except Exception:
            return

        mtype = msg.get('type')

        # Device registration — ESP32/embedded device sends manifest_resp on connect
        # Gateway adds it as a dynamic controller and routes its stream_data
        if mtype == 'manifest_resp':
            ctrl_name = msg.get('device', 'device_{}'.format(len(self._device_clients)))
            # Sanitize name for use as key
            ctrl_name = ctrl_name.replace(' ', '_').replace('-', '_').lower()
            self._device_clients[id(ws)] = {'ws': ws, 'name': ctrl_name, 'latest': {}}
            self._manifests[ctrl_name] = {
                'transport': msg.get('transport', 'websocket'),
                'device':    msg.get('device', ctrl_name),
                'params':    msg.get('params', []),
            }
            log.info('Device registered: {} ({} params)'.format(
                ctrl_name, len(msg.get('params', []))))
            # Push updated manifest to all dashboard clients
            manifest_msg = json.dumps(self._combined_manifest())
            for client in list(self._clients):
                if id(client) not in self._device_clients:
                    try:
                        await client.send(manifest_msg)
                    except Exception:
                        pass
            return

        # Stream data from a registered device — forward to dashboard clients
        if mtype == 'stream_data' and id(ws) in self._device_clients:
            dev = self._device_clients[id(ws)]
            ctrl_name = dev['name']
            data = msg.get('data', {})
            dev['latest'].update(data)
            for k, v in data.items():
                self._last_values['{}.{}'.format(ctrl_name, k)] = v
            # Forward to dashboard clients
            fwd = json.dumps({
                'type': 'stream_data',
                'controller': ctrl_name,
                't': msg.get('t', time.time()),
                'data': data,
            })
            for client in list(self._clients):
                if id(client) not in self._device_clients:
                    try:
                        await client.send(fwd)
                    except Exception:
                        pass
            return

        # write_ack from device — forward to dashboard clients
        if mtype == 'write_ack' and id(ws) in self._device_clients:
            dev = self._device_clients[id(ws)]
            fwd = json.dumps({**msg, 'controller': dev['name']})
            for client in list(self._clients):
                if id(client) not in self._device_clients:
                    try:
                        await client.send(fwd)
                    except Exception:
                        pass
            return

        if mtype == 'manifest_req':
            await ws.send(json.dumps(self._combined_manifest()))

        elif mtype == 'stream_start':
            hz = min(int(msg.get('hz', self._stream_hz)), self.cfg.stream.max_hz)
            self._stream_hz = hz
            log.info('Stream rate: {} Hz'.format(hz))

        elif mtype == 'param_write':
            controller = msg.get('controller')
            name       = msg.get('name')
            value      = msg.get('value')
            if not controller or not name or value is None:
                await ws.send(json.dumps({'type': 'error', 'message': 'param_write requires controller, name, value'}))
                return

            plugin = self._plugins.get(controller)
            manifest = self._manifests.get(controller, {})
            if not plugin:
                await ws.send(json.dumps({'type': 'error', 'message': 'Unknown controller: {}'.format(controller)}))
                return

            params = {p['name']: p for p in manifest.get('params', [])}
            descriptor = params.get(name)
            if not descriptor:
                await ws.send(json.dumps({'type': 'error', 'name': name, 'message': 'Unknown param: {}'.format(name)}))
                return
            if descriptor.get('read_only', False):
                await ws.send(json.dumps({'type': 'error', 'name': name, 'message': '{} is read-only'.format(name)}))
                return

            try:
                key = '{}.{}'.format(controller, name)
                prev = self._last_values.get(key)
                readback = await plugin.write_param(name, value)
                t = time.time()
                await ws.send(json.dumps({
                    'type': 'write_ack', 'controller': controller,
                    'name': name, 'value': readback, 't': t
                }))
                log.info('param_write: {}.{} = {} (readback: {})'.format(
                    controller, name, value, readback))
                if self._logger:
                    self._logger.log_write(t, controller, name, prev, readback,
                                           str(self._remote_addr(ws)))
                self._last_values[key] = readback
            except Exception as e:
                log.error('param_write failed {}.{}: {}'.format(controller, name, e))
                await ws.send(json.dumps({'type': 'error', 'name': name, 'message': str(e)}))

        elif mtype == 'ping':
            await ws.send(json.dumps({'type': 'pong', 't': time.time()}))

        elif mtype == 'session_info':
            if self._logger:
                await ws.send(json.dumps({
                    'type': 'session_info',
                    'stream_file': str(self._logger._stream_path),
                    'writes_file': str(self._logger._writes_path),
                }))

    async def _process_request(self, connection, request):
        """Serve dashboard HTML and MJPEG camera stream for HTTP requests."""
        upgrade = request.headers.get('Upgrade', '')
        if upgrade.lower() == 'websocket':
            return None

        path = str(request.path) if hasattr(request, 'path') else '/'

        # MJPEG camera stream
        if path == '/camera' and self._camera and self._camera.is_running():
            boundary = b'--TuneHUDframe'
            frame = self._camera.get_snapshot_jpeg()
            if frame is None:
                body = b'Camera not ready'
                if WS_V2:
                    return Response(503, 'Service Unavailable', Headers([('Content-Type','text/plain')]), body)
                return (503, [('Content-Type','text/plain')], body)

            # For MJPEG we can only return one frame here — browser will reconnect for next
            # This is enough for a snapshot; true MJPEG needs a raw TCP connection
            if WS_V2:
                headers = Headers([
                    ('Content-Type', 'image/jpeg'),
                    ('Cache-Control', 'no-cache'),
                    ('Refresh', '0'),
                ])
                return Response(200, 'OK', headers, frame)
            return (200, [('Content-Type','image/jpeg'),('Cache-Control','no-cache')], frame)

        # Dashboard HTML
        if os.path.exists(DASHBOARD_FILE):
            with open(DASHBOARD_FILE, 'rb') as f:
                body = f.read()
            if WS_V2:
                headers = Headers([
                    ('Content-Type', 'text/html; charset=utf-8'),
                    ('Content-Length', str(len(body))),
                    ('Cache-Control', 'no-cache'),
                ])
                return Response(200, 'OK', headers, body)
            return (200, [('Content-Type','text/html; charset=utf-8'),('Content-Length',str(len(body)))], body)
        else:
            body = b'<h1>TuneHUD dashboard not found</h1>'
            if WS_V2:
                return Response(404, 'Not Found', Headers([('Content-Type','text/html')]), body)
            return (404, [('Content-Type','text/html')], body)

    async def _mjpeg_server(self, host: str, port: int) -> None:
        """Serve true MJPEG stream on a dedicated port."""
        async def handle_client(reader, writer):
            try:
                # Read HTTP request
                await reader.read(1024)
                # Send MJPEG headers
                writer.write(
                    b'HTTP/1.1 200 OK\r\n'
                    b'Content-Type: multipart/x-mixed-replace; boundary=TuneHUDframe\r\n'
                    b'Cache-Control: no-cache\r\n'
                    b'Connection: close\r\n\r\n'
                )
                await writer.drain()
                while True:
                    frame = self._camera.get_snapshot_jpeg()
                    if frame:
                        chunk = (
                            b'--TuneHUDframe\r\n'
                            b'Content-Type: image/jpeg\r\n'
                            b'Content-Length: ' + str(len(frame)).encode() + b'\r\n\r\n'
                            + frame + b'\r\n'
                        )
                        writer.write(chunk)
                        await writer.drain()
                    await asyncio.sleep(1.0 / self._camera.fps)
            except Exception:
                pass
            finally:
                writer.close()

        server = await asyncio.start_server(handle_client, host, port)
        async with server:
            await server.serve_forever()

    async def _ws_handler(self, ws):
        await self._register(ws)
        try:
            async for raw in ws:
                await self._handle_message(ws, raw)
        except Exception:
            pass
        finally:
            await self._unregister(ws)

    async def _stream_loop(self):
        log.info('Stream loop started at {} Hz'.format(self._stream_hz))
        while True:
            t0 = time.time()

            if self._clients:
                # Read all connected controllers
                combined_data = {}
                for name, plugin in self._plugins.items():
                    if plugin.is_connected():
                        try:
                            data = await plugin.read_all()
                            for k, v in data.items():
                                combined_data['{}.{}'.format(name, k)] = v
                                self._last_values['{}.{}'.format(name, k)] = v
                        except Exception as e:
                            log.warning('Stream read {} failed: {}'.format(name, e))

                if combined_data:
                    if self._logger:
                        if not self._logger._stream_header_written:
                            self._logger.init_stream_header(sorted(combined_data.keys()))
                        self._logger.log_stream(t0, combined_data)

                    # Send per-controller stream_data messages
                    ctrl_data = {}
                    for key, val in combined_data.items():
                        ctrl, param = key.split('.', 1)
                        if ctrl not in ctrl_data:
                            ctrl_data[ctrl] = {}
                        ctrl_data[ctrl][param] = val

                    dead = set()
                    for ctrl, data in ctrl_data.items():
                        msg = json.dumps({
                            'type': 'stream_data',
                            'controller': ctrl,
                            't': t0,
                            'data': data,
                        })
                        for ws in list(self._clients):
                            try:
                                await ws.send(msg)
                            except Exception:
                                dead.add(ws)
                    self._clients -= dead

            elapsed = time.time() - t0
            sleep = (1.0 / self._stream_hz) - elapsed
            if sleep > 0:
                await asyncio.sleep(sleep)

    async def _connect_loop(self):
        while True:
            for name, plugin in self._plugins.items():
                if not plugin.is_connected():
                    try:
                        log.info('Connecting controller: {}'.format(name))
                        await plugin.connect()
                        self._manifests[name] = await plugin.get_manifest()
                        log.info('Controller {} connected. {} params.'.format(
                            name, len(self._manifests[name].get('params', []))))
                        # Push updated manifest to clients
                        if self._clients:
                            manifest_msg = json.dumps(self._combined_manifest())
                            for ws in list(self._clients):
                                try:
                                    await ws.send(manifest_msg)
                                except Exception:
                                    pass
                    except Exception as e:
                        log.error('Controller {} connect failed: {}. Retrying...'.format(name, e))
            await asyncio.sleep(5)

    async def run(self):
        host = self.cfg.server.host
        port = self.cfg.server.port

        log.info('TuneHUD Gateway starting — {}'.format(self.cfg.title))
        log.info('Controllers: {}'.format([c.name for c in self.cfg.controllers]))
        log.info('Server: ws://{}:{}'.format(host, port))
        log.info('Stream: {} Hz (max {} Hz)'.format(self._stream_hz, self.cfg.stream.max_hz))

        if self.cfg.server.serve_dashboard:
            log.info('Dashboard: http://{}:{}/'.format(host if host != '0.0.0.0' else 'localhost', port))

        if self._logger:
            self._logger.open()

        # Start camera if available
        if self._camera:
            if self._camera.start():
                log.info('Camera streaming at http://{}:{}/camera'.format(
                    host if host != '0.0.0.0' else 'localhost', port))
            else:
                log.warning('Camera not available — streaming disabled')
                self._camera = None

        # Initial connect
        for name, plugin in self._plugins.items():
            try:
                await plugin.connect()
                self._manifests[name] = await plugin.get_manifest()
                log.info('Controller {} connected. {} params.'.format(
                    name, len(self._manifests[name].get('params', []))))
            except Exception as e:
                log.warning('Controller {} initial connect failed: {}'.format(name, e))

        serve_kwargs = {}
        if self.cfg.server.serve_dashboard:
            serve_kwargs['process_request'] = self._process_request

        async with ws_serve(self._ws_handler, host, port, **serve_kwargs):
            log.info('WebSocket server listening on ws://{}:{}'.format(host, port))

            # Start MJPEG server after WebSocket is up
            mjpeg_task = None
            if self._camera and self._camera.is_running():
                mjpeg_port = 8769
                mjpeg_task = asyncio.ensure_future(self._mjpeg_server(host, mjpeg_port))
                log.info('MJPEG stream: http://{}:{}/camera'.format(
                    host if host != '0.0.0.0' else 'localhost', mjpeg_port))

            try:
                tasks = [self._stream_loop(), self._connect_loop()]
                if mjpeg_task:
                    tasks.append(mjpeg_task)
                await asyncio.gather(*tasks)
            finally:
                if self._logger:
                    self._logger.close()
                if self._camera:
                    self._camera.stop()


def main():
    parser = argparse.ArgumentParser(description='TuneHUD Gateway')
    parser.add_argument('--config',  required=True)
    parser.add_argument('--log-dir', default='sessions')
    parser.add_argument('--no-log',  action='store_true')
    parser.add_argument('--verbose', action='store_true')
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    )
    # Suppress noisy websockets HTTP rejection logs
    logging.getLogger('websockets.server').setLevel(logging.WARNING)
    logging.getLogger('websockets.asyncio.server').setLevel(logging.WARNING)

    cfg = load_config(args.config)
    log_dir = None if args.no_log else args.log_dir
    gateway = TuneHUDGateway(cfg, log_dir=log_dir)

    try:
        asyncio.run(gateway.run())
    except KeyboardInterrupt:
        log.info('Gateway stopped')


if __name__ == '__main__':
    main()
