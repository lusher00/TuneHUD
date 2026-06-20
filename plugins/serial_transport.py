# Copyright (c) 2025 Ryan Lush <ryan.lush@gmail.com>
#
# Free for personal, educational, and open-source use.
# Commercial use requires written permission from the author.
# Contact: ryan.lush@gmail.com
# SPDX-License-Identifier: MIT
# Copyright (c) 2025 Ryan Lush <ryan.lush@gmail.com>
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
"""
tunehud_gateway/plugins/serial_transport.py
Serial/UART transport — Python 3.7 compatible.
"""
from __future__ import annotations
import asyncio
import json
import logging
from typing import Any, Optional

try:
    import serial_asyncio
    HAS_SERIAL_ASYNCIO = True
except ImportError:
    HAS_SERIAL_ASYNCIO = False

from .base import TransportPlugin, ParamDescriptor

try:
    from config import load_map
except ImportError:
    import sys, os
    sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
    from config import load_map

log = logging.getLogger('tunehud.serial')


class SerialTransport(TransportPlugin):
    def __init__(self, config):
        super().__init__(config)
        self._reader = None
        self._writer = None
        self._param_map = {}
        self._descriptors = []
        self._latest = {}
        self._connected = False
        self._recv_task = None
        self._frame_format = config.get('frame_format', 'json')
        self._csv_headers = []
        map_path = config.get('map')
        if map_path:
            self._load_map(load_map(map_path))

    def _load_map(self, raw_map):
        for name, entry in raw_map.get('params', {}).items():
            self._param_map[name] = entry
            self._descriptors.append(ParamDescriptor(
                name=name, label=entry.get('label', name),
                type=entry.get('type', 'float'), units=entry.get('units'),
                min=entry.get('min'), max=entry.get('max'), step=entry.get('step'),
                read_only=entry.get('read_only', False), group=entry.get('group'),
                description=entry.get('description'),
            ))

    async def connect(self):
        if not HAS_SERIAL_ASYNCIO:
            raise ImportError('pyserial-asyncio not installed')
        port     = self.config.get('port', '/dev/ttyUSB0')
        baudrate = int(self.config.get('baudrate', 115200))
        log.info('Opening serial {} at {}'.format(port, baudrate))
        self._reader, self._writer = await serial_asyncio.open_serial_connection(
            url=port, baudrate=baudrate)
        self._connected = True
        self._recv_task = asyncio.ensure_future(self._recv_loop())

    async def disconnect(self):
        self._connected = False
        if self._recv_task:
            self._recv_task.cancel()
        if self._writer:
            self._writer.close()

    async def _recv_loop(self):
        try:
            while self._connected:
                line = await self._reader.readline()
                if not line:
                    continue
                text = line.decode('utf-8', errors='replace').strip()
                if not text:
                    continue
                if self._frame_format == 'json':
                    try:
                        data = json.loads(text)
                        if isinstance(data, dict):
                            self._latest.update(data)
                    except json.JSONDecodeError:
                        pass
                elif self._frame_format == 'csv':
                    parts = text.split(',')
                    if not self._csv_headers:
                        self._csv_headers = [p.strip() for p in parts]
                    else:
                        try:
                            for k, v in zip(self._csv_headers, parts):
                                self._latest[k] = float(v.strip())
                        except ValueError:
                            pass
        except asyncio.CancelledError:
            pass
        except Exception as e:
            if self._connected:
                log.warning('Serial recv error: {}'.format(e))
            self._connected = False

    async def get_manifest(self):
        return {
            'transport': 'serial',
            'device': self.config.get('device', 'Serial Device'),
            'version': '1.0',
            'params': [d.to_dict() for d in self._descriptors],
        }

    async def read_param(self, name):
        return self._latest.get(name)

    async def write_param(self, name, value):
        if not self._writer:
            raise RuntimeError('Not connected')
        entry = self._param_map.get(name, {})
        if entry.get('read_only', False):
            raise PermissionError('{} is read-only'.format(name))
        cmd = json.dumps({'name': name, 'value': value}) + '\n'
        self._writer.write(cmd.encode())
        await self._writer.drain()
        self._latest[name] = value
        return value

    async def read_all(self):
        return dict(self._latest)

    def is_connected(self):
        return self._connected
