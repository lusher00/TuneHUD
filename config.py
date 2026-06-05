from __future__ import annotations
import yaml
from dataclasses import dataclass, field
from typing import Optional, List


@dataclass
class StreamConfig:
    max_hz:     int = 100
    default_hz: int = 20


@dataclass
class ServerConfig:
    host:           str  = '0.0.0.0'
    port:           int  = 8765
    serve_dashboard: bool = True   # serve tunehud_dashboard.html from gateway


@dataclass
class ControllerConfig:
    """One transport/device connection."""
    name:    str
    type:    str                        # websocket | modbus_tcp | modbus_rtu | serial
    options: dict = field(default_factory=dict)
    map:     Optional[str] = None

    # Display hints for gauge cluster
    gauges:  List[dict] = field(default_factory=list)


@dataclass
class GatewayConfig:
    title:       str
    stream:      StreamConfig
    server:      ServerConfig
    controllers: List[ControllerConfig]


def load_config(path):
    with open(path) as f:
        raw = yaml.safe_load(f)

    stream = StreamConfig(**raw.get('stream', {}))
    server_raw = raw.get('server', {})
    server = ServerConfig(**server_raw)

    controllers = []
    for c in raw.get('controllers', []):
        gauges = c.pop('gauges', [])
        map_path = c.pop('map', None)
        name = c.pop('name')
        ctype = c.pop('type')
        controllers.append(ControllerConfig(
            name=name,
            type=ctype,
            options=c,
            map=map_path,
            gauges=gauges,
        ))

    # Legacy single-transport support
    if not controllers and 'transport' in raw:
        t = raw['transport']
        gauges = t.pop('gauges', [])
        map_path = t.pop('map', None)
        name = t.pop('name', t.get('type', 'device'))
        ctype = t.pop('type')
        controllers.append(ControllerConfig(
            name=name, type=ctype, options=t,
            map=map_path, gauges=gauges,
        ))

    return GatewayConfig(
        title=raw.get('title', raw.get('device', 'TuneHUD')),
        stream=stream,
        server=server,
        controllers=controllers,
    )


def load_map(path):
    with open(path) as f:
        return yaml.safe_load(f)
