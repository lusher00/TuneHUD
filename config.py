from __future__ import annotations
import yaml
from dataclasses import dataclass, field
from typing import Optional, List


@dataclass
class StreamConfig:
    max_hz:     int = 100
    default_hz: int = 50


@dataclass
class ServerConfig:
    host: str = '0.0.0.0'
    port: int = 8765


@dataclass
class TransportConfig:
    type:    str
    name:    str = ''
    options: dict = field(default_factory=dict)
    map:     Optional[str] = None


@dataclass
class GaugeConfig:
    param:     str
    type:      str = 'numeric'
    min:       Optional[float] = None
    max:       Optional[float] = None


@dataclass
class DisplayConfig:
    gauges: List[GaugeConfig] = field(default_factory=list)
    plots:  List[List[str]] = field(default_factory=list)


@dataclass
class GatewayConfig:
    device:    str
    stream:    StreamConfig
    server:    ServerConfig
    transport: TransportConfig
    display:   DisplayConfig


def load_config(path):
    with open(path) as f:
        raw = yaml.safe_load(f)
    stream = StreamConfig(**raw.get('stream', {}))
    server = ServerConfig(**raw.get('server', {}))
    t = raw.get('transport', {})
    transport = TransportConfig(
        type=t['type'],
        name=t.get('name', t['type']),
        options={k: v for k, v in t.items() if k not in ('type', 'name', 'map')},
        map=t.get('map'),
    )
    d = raw.get('display', {})
    gauges = [GaugeConfig(**g) for g in d.get('gauges', [])]
    display = DisplayConfig(gauges=gauges, plots=d.get('plots', []))
    return GatewayConfig(
        device=raw.get('device', 'Unknown Device'),
        stream=stream, server=server, transport=transport, display=display,
    )


def load_map(path):
    with open(path) as f:
        return yaml.safe_load(f)
