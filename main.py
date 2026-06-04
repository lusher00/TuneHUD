"""
tunehud_gateway/main.py
TuneHUD Gateway — Python 3.7, websockets 9-11 compatible.

Usage:
  python3 main.py --config configs/demo.yaml
  python3 main.py --config configs/demo.yaml --verbose
  python3 main.py --config configs/demo.yaml --no-log
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
from datetime import datetime
from pathlib import Path

import websockets

sys.path.insert(0, os.path.dirname(__file__))

from config import load_config
from plugins import get_plugin

log = logging.getLogger('tunehud.gateway')


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
        self._writes_writer.writerow(['t', 'name', 'prev_value', 'new_value', 'client'])
        self._writes_file.flush()

    def init_stream_header(self, param_names):
        if self._stream_header_written or not self._stream_file:
            return
        self._stream_keys = param_names
        self._stream_writer = csv.writer(self._stream_file)
        self._stream_writer.writerow(['t'] + param_names)
        self._stream_file.flush()
        self._stream_header_written = True

    def log_stream(self, t, data):
        if not self._stream_writer:
            return
        self._stream_writer.writerow(
            ['{:.3f}'.format(t)] + [str(data.get(k, '')) for k in self._stream_keys])
        self._stream_file.flush()

    def log_write(self, t, name, prev, new, client):
        if not self._writes_writer:
            return
        self._writes_writer.writerow(['{:.3f}'.format(t), name, prev, new, client])
        self._writes_file.flush()

    def close(self):
        if self._stream_file:
            self._stream_file.close()
        if self._writes_file:
            self._writes_file.close()


class TuneHUDGateway:
    def __init__(self, config, log_dir=None):
        self.cfg = config
        self.plugin = get_plugin(
            config.transport.type,
            dict(list(config.transport.options.items()) + [
                ('name', config.transport.name),
                ('device', config.device),
                ('map', config.transport.map),
            ]),
        )
        self._clients = set()
        self._manifest = {}
        self._stream_hz = min(config.stream.default_hz, config.stream.max_hz)
        self._logger = SessionLogger(log_dir) if log_dir else None
        self._last_values = {}

    async def _register(self, ws):
        self._clients.add(ws)
        log.info('Client connected: {} ({} total)'.format(ws.remote_address, len(self._clients)))
        if self._manifest:
            await ws.send(json.dumps(dict({'type': 'manifest_resp'}, **self._manifest)))

    async def _unregister(self, ws):
        self._clients.discard(ws)
        log.info('Client disconnected ({} remaining)'.format(len(self._clients)))

    async def _handle_message(self, ws, raw):
        try:
            msg = json.loads(raw)
        except Exception:
            await ws.send(json.dumps({'type': 'error', 'message': 'Invalid JSON'}))
            return

        mtype = msg.get('type')

        if mtype == 'manifest_req':
            await ws.send(json.dumps(dict({'type': 'manifest_resp'}, **self._manifest)))

        elif mtype == 'stream_start':
            hz = min(int(msg.get('hz', self._stream_hz)), self.cfg.stream.max_hz)
            self._stream_hz = hz
            log.info('Stream rate: {} Hz'.format(hz))

        elif mtype == 'param_write':
            name  = msg.get('name')
            value = msg.get('value')
            if name is None or value is None:
                await ws.send(json.dumps({'type': 'error', 'message': 'param_write requires name and value'}))
                return
            params = {p['name']: p for p in self._manifest.get('params', [])}
            descriptor = params.get(name)
            if not descriptor:
                await ws.send(json.dumps({'type': 'error', 'name': name, 'message': 'Unknown param: {}'.format(name)}))
                return
            if descriptor.get('read_only', False):
                await ws.send(json.dumps({'type': 'error', 'name': name, 'message': '{} is read-only'.format(name)}))
                return
            try:
                prev = self._last_values.get(name)
                readback = await self.plugin.write_param(name, value)
                t = time.time()
                await ws.send(json.dumps({'type': 'write_ack', 'name': name, 'value': readback, 't': t}))
                log.info('param_write: {} = {} (readback: {})'.format(name, value, readback))
                if self._logger:
                    self._logger.log_write(t, name, prev, readback, str(ws.remote_address))
                self._last_values[name] = readback
            except Exception as e:
                log.error('param_write failed: {} = {}: {}'.format(name, value, e))
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

    async def _ws_handler(self, ws, path=None):
        await self._register(ws)
        try:
            async for raw in ws:
                await self._handle_message(ws, raw)
        except websockets.exceptions.ConnectionClosed:
            pass
        finally:
            await self._unregister(ws)

    async def _stream_loop(self):
        log.info('Stream loop started at {} Hz'.format(self._stream_hz))
        while True:
            t0 = time.time()
            if self.plugin.is_connected():
                try:
                    data = await self.plugin.read_all()
                    self._last_values.update(data)
                    if self._logger and data:
                        if not self._logger._stream_header_written:
                            self._logger.init_stream_header(sorted(data.keys()))
                        self._logger.log_stream(t0, data)
                    if self._clients:
                        msg = json.dumps({'type': 'stream_data', 't': t0, 'data': data})
                        dead = set()
                        for ws in list(self._clients):
                            try:
                                await ws.send(msg)
                            except websockets.exceptions.ConnectionClosed:
                                dead.add(ws)
                        self._clients -= dead
                except Exception as e:
                    log.warning('Stream read error: {}'.format(e))
            elapsed = time.time() - t0
            sleep = (1.0 / self._stream_hz) - elapsed
            if sleep > 0:
                await asyncio.sleep(sleep)

    async def _connect_loop(self):
        while True:
            if not self.plugin.is_connected():
                try:
                    log.info('Connecting transport: {}'.format(self.cfg.transport.type))
                    await self.plugin.connect()
                    self._manifest = await self.plugin.get_manifest()
                    log.info('Transport connected. {} params.'.format(
                        len(self._manifest.get('params', []))))
                    if self._clients:
                        manifest_msg = json.dumps(dict({'type': 'manifest_resp'}, **self._manifest))
                        for ws in list(self._clients):
                            try:
                                await ws.send(manifest_msg)
                            except Exception:
                                pass
                except Exception as e:
                    log.error('Transport connect failed: {}. Retrying in 5s...'.format(e))
                    await asyncio.sleep(5)
                    continue
            await asyncio.sleep(2)

    async def run(self):
        host = self.cfg.server.host
        port = self.cfg.server.port

        log.info('TuneHUD Gateway starting')
        log.info('Device:    {}'.format(self.cfg.device))
        log.info('Transport: {}'.format(self.cfg.transport.type))
        log.info('Server:    ws://{}:{}'.format(host, port))
        log.info('Stream:    {} Hz (max {} Hz)'.format(self._stream_hz, self.cfg.stream.max_hz))

        if self._logger:
            self._logger.open()

        try:
            log.info('Connecting transport...')
            await self.plugin.connect()
            self._manifest = await self.plugin.get_manifest()
            log.info('Transport connected. {} params.'.format(
                len(self._manifest.get('params', []))))
        except Exception as e:
            log.warning('Initial connect failed: {}. Will retry...'.format(e))

        async with websockets.serve(self._ws_handler, host, port):
            log.info('WebSocket server listening on ws://{}:{}'.format(host, port))
            try:
                await asyncio.gather(self._stream_loop(), self._connect_loop())
            finally:
                if self._logger:
                    self._logger.close()


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

    cfg = load_config(args.config)
    log_dir = None if args.no_log else args.log_dir
    gateway = TuneHUDGateway(cfg, log_dir=log_dir)

    try:
        asyncio.run(gateway.run())
    except KeyboardInterrupt:
        log.info('Gateway stopped')


if __name__ == '__main__':
    main()
