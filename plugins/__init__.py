from __future__ import annotations
from .base import TransportPlugin, ParamDescriptor
from .websocket import WebSocketTransport
from .modbus_tcp import ModbusTCPTransport
from .modbus_rtu import ModbusRTUTransport
from .serial_transport import SerialTransport

REGISTRY = {
    'websocket':  WebSocketTransport,
    'modbus_tcp': ModbusTCPTransport,
    'modbus_rtu': ModbusRTUTransport,
    'serial':     SerialTransport,
}

def get_plugin(transport_type, config):
    cls = REGISTRY.get(transport_type)
    if cls is None:
        raise ValueError('Unknown transport: {}. Available: {}'.format(
            transport_type, list(REGISTRY.keys())))
    return cls(config)
