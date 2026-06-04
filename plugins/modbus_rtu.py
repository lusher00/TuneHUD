"""
tunehud_gateway/plugins/modbus_rtu.py
Modbus RTU transport — pymodbus 2.x compatible.
"""
from __future__ import annotations
import asyncio
import logging
from typing import Any, Optional

from pymodbus.client.sync import ModbusSerialClient
from pymodbus.exceptions import ModbusException

from .base import TransportPlugin, ParamDescriptor
from .modbus_tcp import _decode, _encode, TYPE_WORDS

try:
    from config import load_map
except ImportError:
    import sys, os
    sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
    from config import load_map

log = logging.getLogger('tunehud.modbus_rtu')


class ModbusRTUTransport(TransportPlugin):
    def __init__(self, config):
        super().__init__(config)
        self._client = None
        self._param_map = {}
        self._descriptors = []
        self._connected = False
        self._unit_id = int(config.get('unit_id', 1))
        map_path = config.get('map')
        if map_path:
            self._load_map(load_map(map_path))

    def _load_map(self, raw_map):
        for name, entry in raw_map.get('params', {}).items():
            entry['_name'] = name
            self._param_map[name] = entry
            dtype = entry.get('type', 'float32')
            ptype = 'float'
            if dtype in ('bool',): ptype = 'bool'
            elif dtype in ('int16','int32','uint16','uint32'): ptype = 'int'
            self._descriptors.append(ParamDescriptor(
                name=name, label=entry.get('label', name), type=ptype,
                units=entry.get('units'), min=entry.get('min'), max=entry.get('max'),
                step=entry.get('step'), read_only=entry.get('read_only', False),
                group=entry.get('group'), description=entry.get('description'),
            ))

    async def connect(self):
        port     = self.config.get('port', '/dev/ttyUSB0')
        baudrate = int(self.config.get('baudrate', 9600))
        parity   = self.config.get('parity', 'N')
        stopbits = int(self.config.get('stopbits', 1))
        bytesize = int(self.config.get('bytesize', 8))
        log.info('Connecting Modbus RTU {} {}'.format(port, baudrate))
        loop = asyncio.get_event_loop()
        self._client = ModbusSerialClient(
            method='rtu', port=port, baudrate=baudrate,
            parity=parity, stopbits=stopbits, bytesize=bytesize)
        connected = await loop.run_in_executor(None, self._client.connect)
        if not connected:
            raise ConnectionError('Failed to connect Modbus RTU on {}'.format(port))
        self._connected = True
        log.info('Modbus RTU connected')

    async def disconnect(self):
        if self._client:
            self._client.close()
        self._connected = False

    async def get_manifest(self):
        return {
            'transport': 'modbus_rtu',
            'device': self.config.get('device', 'Modbus RTU Device'),
            'version': '1.0',
            'params': [d.to_dict() for d in self._descriptors],
        }

    async def _read_register(self, entry):
        reg = entry['register']
        dtype = entry.get('type', 'float32')
        words = TYPE_WORDS.get(dtype, 1)
        scale = float(entry.get('scale', 1.0))
        offset = float(entry.get('offset', 0.0))
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(None,
            lambda: self._client.read_holding_registers(reg, count=words, unit=self._unit_id))
        if result is None or result.isError():
            raise ModbusException('Read error at register {}'.format(reg))
        return _decode(result.registers, dtype) * scale + offset

    async def read_param(self, name):
        entry = self._param_map.get(name)
        if not entry: raise KeyError('Unknown param: {}'.format(name))
        return await self._read_register(entry)

    async def write_param(self, name, value):
        entry = self._param_map.get(name)
        if not entry: raise KeyError('Unknown param: {}'.format(name))
        if entry.get('read_only', False): raise PermissionError('{} is read-only'.format(name))
        reg = entry['register']
        dtype = entry.get('type', 'float32')
        scale = float(entry.get('scale', 1.0))
        offset = float(entry.get('offset', 0.0))
        raw_val = (float(value) - offset) / scale
        words = _encode(raw_val, dtype)
        loop = asyncio.get_event_loop()
        if len(words) == 1:
            await loop.run_in_executor(None, lambda: self._client.write_register(reg, words[0], unit=self._unit_id))
        else:
            await loop.run_in_executor(None, lambda: self._client.write_registers(reg, words, unit=self._unit_id))
        return await self._read_register(entry)

    async def read_all(self):
        results = {}
        for name, entry in self._param_map.items():
            try:
                results[name] = await self._read_register(entry)
            except Exception as e:
                log.warning('read_all: {} failed: {}'.format(name, e))
        return results

    def is_connected(self):
        return self._connected and self._client is not None
