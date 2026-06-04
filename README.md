# TuneHUD Gateway

Python gateway that connects to industrial hardware and serves the TuneHUD WebSocket protocol to iOS, Android, and web clients.

## Quick start — demo mode

```bash
# Install dependencies
pip3 install -r requirements.txt

# Terminal 1 — start demo node (simulated PID plant)
python3 demo_node.py

# Terminal 2 — start gateway
python3 main.py --config configs/demo.yaml

# Open dashboard in browser
open tunehud_dashboard.html
```

Connect to `ws://localhost:8765` from the dashboard Settings tab.

## Directory structure

```
tunehud_gateway/
  main.py                      # Gateway entry point
  demo_node.py                 # Simulated PID plant for testing
  config.py                    # YAML config loader
  requirements.txt
  tunehud_dashboard.html       # Web dashboard
  tunehud.service              # systemd service file
  plugins/
    base.py                    # TransportPlugin ABC + ParamDescriptor
    __init__.py                # Plugin registry
    websocket.py               # WebSocket transport (native TuneHUD devices)
    modbus_tcp.py              # Modbus TCP
    modbus_rtu.py              # Modbus RTU (RS485)
    serial_transport.py        # Serial/UART (JSON or CSV frames)
  configs/
    demo.yaml                  # Demo node
    modbus_example.yaml        # Modbus TCP example
    modbus_rtu_example.yaml    # Modbus RTU example
    serial_example.yaml        # Serial/UART example
  maps/
    modbus_example.yaml        # Modbus register map
    serial_example.yaml        # Serial param map
    can_example.yaml           # CAN signal map (future)
```

## Supported transports

| Transport    | Config type  | Hardware needed            |
|--------------|--------------|----------------------------|
| WebSocket    | websocket    | None — standard Ethernet   |
| Modbus TCP   | modbus_tcp   | None — standard Ethernet   |
| Modbus RTU   | modbus_rtu   | USB-RS485 adapter (~$10)   |
| Serial/UART  | serial       | USB-serial adapter (~$5)   |

## Config YAML format

```yaml
device: "My Device"

server:
  host: 0.0.0.0
  port: 8765

stream:
  max_hz:     100
  default_hz: 20

transport:
  type:    modbus_tcp      # websocket | modbus_tcp | modbus_rtu | serial
  name:    my_plc
  host:    192.168.1.50   # modbus_tcp/websocket
  port:    502
  unit_id: 1
  map:     maps/my_plc.yaml

display:
  gauges:
    - param: pv
      type: gauge
      min: 0
      max: 100
  plots:
    - [pv, setpoint]
    - [error, output]
```

## Modbus register map

```yaml
params:
  kp:
    register:  100        # holding register (0-based)
    type:      float32    # float32|float16|int16|uint16|int32|uint32|bool
    scale:     1.0        # value = raw * scale + offset
    offset:    0.0
    read_only: false
    label:     "Proportional gain"
    units:     null
    min:       0.0
    max:       10.0
    step:      0.01
    group:     "PID gains"
```

## Serial map

```yaml
params:
  kp:
    read_only: false
    label: "Proportional gain"
    type:  float
    min:   0.0
    max:   10.0
    step:  0.01
    group: "PID gains"
```

Device sends JSON each line: `{"kp":1.25,"pv":74.3}`  
TuneHUD writes: `{"name":"kp","value":1.5}`

## systemd install (Raspberry Pi)

```bash
sudo cp tunehud.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable tunehud
sudo systemctl start tunehud
```

Edit `tunehud.service` to set the correct user and config path.

## Session logging

Sessions are logged to `sessions/` by default:
- `sessions/session_YYYYMMDD_HHMMSS.csv` — timestamped stream data
- `sessions/session_YYYYMMDD_HHMMSS_writes.csv` — param write audit trail

Disable with `--no-log`. Change directory with `--log-dir /path/to/logs`.

## Gateway CLI

```
python3 main.py --config configs/demo.yaml          # run with session logging
python3 main.py --config configs/demo.yaml --no-log # disable logging
python3 main.py --config configs/demo.yaml --log-dir /var/log/tunehud
python3 main.py --config configs/demo.yaml --verbose
```
