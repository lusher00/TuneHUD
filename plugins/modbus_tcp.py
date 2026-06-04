"""
tunehud_gateway/plugins/modbus_tcp.py
Modbus TCP — compatible with pymodbus 2.x and 3.x
"""
from __future__ import annotations
import asyncio
import logging
import struct
import sys

import pymodbus
PYMODBUS_V3 = int(pymodbus.__version__.split('.')[0]) >= 3

if PYMODBUS_V3:
    from pymodbus.client import AsyncModbusTcpClient
else:
    from pymodbus.client.sync import ModbusTcpClient

from pymodbus.exceptions import ModbusException
from .base import TransportPlugin, ParamDescriptor

try:
    from config import load_map
except ImportError:
    import os
    sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
    from config import load_map

log = logging.getLogger('tunehud.modbus_tcp')

TYPE_WORDS = {'float32':2,'float16':1,'int32':2,'uint32':2,'int16':1,'uint16':1,'bool':1}

def _decode(registers, dtype):
    if dtype == 'float32':
        return struct.unpack('>f', struct.pack('>HH', registers[0], registers[1]))[0]
    elif dtype == 'float16':
        return struct.unpack('>e', struct.pack('>H', registers[0]))[0]
    elif dtype == 'int16':
        return float(struct.unpack('>h', struct.pack('>H', registers[0]))[0])
    elif dtype == 'uint16':
        return float(registers[0])
    elif dtype == 'int32':
        return float(struct.unpack('>i', struct.pack('>HH', registers[0], registers[1]))[0])
    elif dtype == 'uint32':
        return float(struct.unpack('>I', struct.pack('>HH', registers[0], registers[1]))[0])
    elif dtype == 'bool':
        return float(registers[0] != 0)
    return float(registers[0])

def _encode(value, dtype):
    if dtype == 'float32':
        return list(struct.unpack('>HH', struct.pack('>f', float(value))))
    elif dtype == 'float16':
        return [struct.unpack('>H', struct.pack('>e', float(value)))[0]]
    elif dtype == 'int16':
        return [struct.unpack('>H', struct.pack('>h', int(value)))[0]]
    elif dtype == 'uint16':
        return [int(value) & 0xFFFF]
    elif dtype == 'int32':
        return list(struct.unpack('>HH', struct.pack('>i', int(value))))
    elif dtype == 'uint32':
        return list(struct.unpack('>HH', struct.pack('>I', int(value))))
    elif dtype == 'bool':
        return [1 if value else 0]
    return [int(value)]


class ModbusTCPTransport(TransportPlugin):
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
            ptype = 'bool' if dtype == 'bool' else ('int' if dtype in ('int16','int32','uint16','uint32') else 'float')
            self._descriptors.append(ParamDescriptor(
                name=name, label=entry.get('label', name), type=ptype,
                units=entry.get('units'), min=entry.get('min'), max=entry.get('max'),
                step=entry.get('step'), read_only=entry.get('read_only', False),
                group=entry.get('group'), description=entry.get('description'),
            ))

    async def connect(self):
        host = self.config.get('host', 'localhost')
        port = int(self.config.get('port', 502))
        log.info('Connecting Modbus TCP {}:{}'.format(host, port))
        if PYMODBUS_V3:
            self._client = AsyncModbusTcpClient(host, port=port)
            await self._client.connect()
            self._connected = self._client.connected
        else:
            loop = asyncio.get_event_loop()
            self._client = ModbusTcpClient(host, port=port)
            self._connected = await loop.run_in_executor(None, self._client.connect)
        if not self._connected:
            raise ConnectionError('Failed to connect Modbus TCP {}:{}'.format(host, port))
        log.info('Modbus TCP connected')

    async def disconnect(self):
        if self._client:
            if PYMODBUS_V3:
                self._client.close()
            else:
                self._client.close()
        self._connected = False

    async def get_manifest(self):
        return {
            'transport': 'modbus_tcp',
            'device': self.config.get('device', 'Modbus Device'),
            'version': '1.0',
            'params': [d.to_dict() for d in self._descriptors],
        }

    async def _read_register(self, entry):
        reg = entry['register']
        dtype = entry.get('type', 'float32')
        words = TYPE_WORDS.get(dtype, 1)
        scale = float(entry.get('scale', 1.0))
        offset = float(entry.get('offset', 0.0))
        if PYMODBUS_V3:
            result = await self._client.read_holding_registers(reg, count=words, slave=self._unit_id)
        else:
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
        if PYMODBUS_V3:
            if len(words) == 1:
                await self._client.write_register(reg, words[0], slave=self._unit_id)
            else:
                await self._client.write_registers(reg, words, slave=self._unit_id)
        else:
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
