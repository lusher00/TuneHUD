"""
tunehud_gateway/plugins/can_bus.py
CAN bus transport plugin using python-can + SocketCAN.

Signal map YAML format:
  params:
    engine_rpm:
      can_id:    0x100        # CAN frame ID (hex or decimal)
      start_bit: 0            # start bit in frame data (little-endian)
      length:    16           # bit length
      dtype:     uint16       # uint8|uint16|uint32|int8|int16|int32|float32
      scale:     0.25         # value = raw * scale + offset
      offset:    0.0
      read_only: true
      label:     "Engine RPM"
      units:     "rpm"
      min:       0
      max:       8000
      group:     "Engine"
    kp:
      can_id:    0x200
      start_bit: 0
      length:    32
      dtype:     float32
      read_only: false
      write_id:  0x201        # optional: separate frame ID for writes
      label:     "Kp"
      group:     "PID"

Config example (gateway config YAML):
  transport:
    type: can_bus
    name: motor_controller
    channel: can0             # SocketCAN interface name
    bitrate: 500000
    map: maps/motor_can.yaml
"""

import asyncio
import logging
import struct
import time
from typing import Any, Optional

import can

from .base import TransportPlugin, ParamDescriptor
from config import load_map

log = logging.getLogger('tunehud.can_bus')


def _extract_bits(data: bytes, start_bit: int, length: int) -> int:
    """Extract an integer value from CAN frame data bytes (little-endian bit ordering)."""
    value = 0
    for i in range(length):
        byte_idx = (start_bit + i) // 8
        bit_idx  = (start_bit + i) % 8
        if byte_idx < len(data):
            if data[byte_idx] & (1 << bit_idx):
                value |= (1 << i)
    return value


def _insert_bits(data: bytearray, start_bit: int, length: int, value: int) -> None:
    """Insert an integer value into CAN frame data bytes (little-endian bit ordering)."""
    for i in range(length):
        byte_idx = (start_bit + i) // 8
        bit_idx  = (start_bit + i) % 8
        if byte_idx < len(data):
            if value & (1 << i):
                data[byte_idx] |= (1 << bit_idx)
            else:
                data[byte_idx] &= ~(1 << bit_idx)


def _decode_raw(raw: int, dtype: str, length: int) -> float:
    if dtype == 'float32':
        return struct.unpack('>f', struct.pack('>I', raw & 0xFFFFFFFF))[0]
    elif dtype == 'int8':
        v = raw & 0xFF
        return float(struct.unpack('b', bytes([v]))[0])
    elif dtype == 'int16':
        v = raw & 0xFFFF
        return float(struct.unpack('>h', struct.pack('>H', v))[0])
    elif dtype == 'int32':
        v = raw & 0xFFFFFFFF
        return float(struct.unpack('>i', struct.pack('>I', v))[0])
    else:  # uint variants
        return float(raw)


def _encode_raw(value: float, dtype: str) -> int:
    if dtype == 'float32':
        return struct.unpack('>I', struct.pack('>f', float(value)))[0]
    elif dtype == 'int8':
        return struct.unpack('B', struct.pack('b', int(value)))[0]
    elif dtype == 'int16':
        return struct.unpack('>H', struct.pack('>h', int(value)))[0]
    elif dtype == 'int32':
        return struct.unpack('>I', struct.pack('>i', int(value)))[0]
    else:
        return int(value)


class CANTransport(TransportPlugin):

    def __init__(self, config: dict):
        super().__init__(config)
        self._bus: Optional[can.Bus] = None
        self._param_map: dict = {}
        self._descriptors: list = []
        self._latest: dict = {}
        self._connected = False
        self._recv_task: Optional[asyncio.Task] = None

        # Map from CAN ID to list of param names — for fast receive dispatch
        self._id_to_params: dict[int, list[str]] = {}

        map_path = config.get('map')
        if map_path:
            self._load_map(load_map(map_path))

    def _load_map(self, raw_map: dict) -> None:
        for name, entry in raw_map.get('params', {}).items():
            entry['_name'] = name
            can_id = int(str(entry['can_id']), 0)  # handle hex strings
            entry['can_id'] = can_id
            self._param_map[name] = entry

            # Build reverse lookup
            if can_id not in self._id_to_params:
                self._id_to_params[can_id] = []
            self._id_to_params[can_id].append(name)

            dtype = entry.get('dtype', 'uint16')
            ptype = 'float'
            if dtype in ('uint8', 'uint16', 'uint32', 'int8', 'int16', 'int32'):
                ptype = 'int'

            self._descriptors.append(ParamDescriptor(
                name=name,
                label=entry.get('label', name),
                type=ptype,
                units=entry.get('units'),
                min=entry.get('min'),
                max=entry.get('max'),
                step=entry.get('step'),
                read_only=entry.get('read_only', True),
                group=entry.get('group'),
                description=entry.get('description'),
            ))

    async def connect(self) -> None:
        channel  = self.config.get('channel', 'can0')
        bitrate  = int(self.config.get('bitrate', 500000))
        bustype  = self.config.get('bustype', 'socketcan')
        log.info(f'Opening CAN bus {channel} at {bitrate} bps ({bustype})')

        self._bus = can.Bus(channel=channel, bustype=bustype, bitrate=bitrate)
        self._connected = True
        log.info('CAN bus connected')

        # Start background receive task
        self._recv_task = asyncio.create_task(self._recv_loop())

    async def disconnect(self) -> None:
        self._connected = False
        if self._recv_task:
            self._recv_task.cancel()
        if self._bus:
            self._bus.shutdown()
            self._bus = None

    async def _recv_loop(self) -> None:
        """Background task — receive CAN frames and update latest values."""
        loop = asyncio.get_event_loop()
        while self._connected:
            try:
                # Run blocking recv in executor to avoid blocking event loop
                msg = await loop.run_in_executor(
                    None, lambda: self._bus.recv(timeout=0.05)
                )
                if msg is None:
                    continue
                params = self._id_to_params.get(msg.arbitration_id, [])
                for name in params:
                    entry = self._param_map[name]
                    raw = _extract_bits(msg.data, entry['start_bit'], entry['length'])
                    val = _decode_raw(raw, entry.get('dtype', 'uint16'), entry['length'])
                    scale  = float(entry.get('scale', 1.0))
                    offset = float(entry.get('offset', 0.0))
                    self._latest[name] = val * scale + offset
            except Exception as e:
                if self._connected:
                    log.warning(f'CAN recv error: {e}')
                await asyncio.sleep(0.01)

    async def get_manifest(self) -> dict:
        return {
            'transport': 'can_bus',
            'device':    self.config.get('device', 'CAN Device'),
            'version':   '1.0',
            'params':    [d.to_dict() for d in self._descriptors],
        }

    async def read_param(self, name: str) -> Any:
        return self._latest.get(name)

    async def write_param(self, name: str, value: Any) -> Any:
        entry = self._param_map.get(name)
        if not entry:
            raise KeyError(f'Unknown param: {name}')
        if entry.get('read_only', True):
            raise PermissionError(f'{name} is read-only')

        scale  = float(entry.get('scale', 1.0))
        offset = float(entry.get('offset', 0.0))
        raw_val = (float(value) - offset) / scale

        dtype    = entry.get('dtype', 'uint16')
        raw_int  = _encode_raw(raw_val, dtype)
        length   = entry['length']
        write_id = int(str(entry.get('write_id', entry['can_id'])), 0)

        # Build 8-byte frame
        data = bytearray(8)
        _insert_bits(data, entry['start_bit'], length, raw_int)

        msg = can.Message(arbitration_id=write_id, data=bytes(data), is_extended_id=False)
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self._bus.send, msg)

        self._latest[name] = value
        return value

    async def read_all(self) -> dict[str, Any]:
        return dict(self._latest)

    def is_connected(self) -> bool:
        return self._connected
