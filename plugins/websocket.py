"""
tunehud_gateway/plugins/websocket.py
WebSocket transport — websockets 9-11 compatible.
"""
from __future__ import annotations
import asyncio
import json
import logging
import time
from typing import Any, Optional

import websockets

from .base import TransportPlugin, ParamDescriptor

log = logging.getLogger('tunehud.ws_transport')


class WebSocketTransport(TransportPlugin):
    def __init__(self, config):
        super().__init__(config)
        self._ws = None
        self._manifest_cache = None
        self._latest = {}
        self._connected = False
        self._recv_task = None

    @property
    def host(self):
        return self.config.get('host', 'localhost')

    @property
    def port(self):
        return int(self.config.get('port', 8766))

    async def connect(self):
        uri = 'ws://{}:{}'.format(self.host, self.port)
        log.info('Connecting to {}'.format(uri))
        self._ws = await websockets.connect(uri)
        self._connected = True
        await self._ws.send(json.dumps({'type': 'manifest_req'}))
        raw = await self._ws.recv()
        msg = json.loads(raw)
        if msg.get('type') == 'manifest_resp':
            self._manifest_cache = {k: v for k, v in msg.items() if k != 'type'}
            log.info('Manifest received: {} params'.format(len(self._manifest_cache.get('params', []))))
        self._recv_task = asyncio.ensure_future(self._recv_loop())

    async def disconnect(self):
        self._connected = False
        if self._recv_task:
            self._recv_task.cancel()
        if self._ws:
            await self._ws.close()
            self._ws = None

    async def _recv_loop(self):
        try:
            async for raw in self._ws:
                try:
                    msg = json.loads(raw)
                    if msg.get('type') == 'stream_data':
                        self._latest.update(msg.get('data', {}))
                except Exception as e:
                    log.warning('Parse error: {}'.format(e))
        except Exception:
            self._connected = False

    async def get_manifest(self):
        if self._manifest_cache:
            return self._manifest_cache
        raise RuntimeError('Manifest not available')

    async def read_param(self, name):
        return self._latest.get(name)

    async def write_param(self, name, value):
        if not self._ws:
            raise RuntimeError('Not connected')
        await self._ws.send(json.dumps({'type': 'param_write', 'name': name, 'value': value}))
        deadline = time.time() + 0.5
        while time.time() < deadline:
            await asyncio.sleep(0.01)
            if name in self._latest:
                return self._latest[name]
        return value

    async def read_all(self):
        return dict(self._latest)

    def is_connected(self):
        return self._connected
