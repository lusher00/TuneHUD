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
tunehud_gateway/demo_node.py
TuneHUD Demo Node — simulates a second-order plant with PID controller.

Runs standalone. Speaks TuneHUD WebSocket protocol natively so it can be
used as a 'websocket' transport target by the gateway, or connected to
directly by a TuneHUD client.

Usage:
  python3 demo_node.py                   # default port 8766
  python3 demo_node.py --port 8766
  python3 demo_node.py --verbose

The simulated system:
  - Second-order plant: motor velocity loop
  - PID controller running at 100 Hz internally
  - Params: kp, ki, kd, setpoint, pv (read-only), output (read-only),
            error (read-only), disturbance (inject a step disturbance)
  - Realistic behaviour: too-high Kp causes oscillation, step response visible
"""

import asyncio
import json
import logging
import argparse
import time
import math
import random

import websockets
from websockets.server import WebSocketServerProtocol

log = logging.getLogger('tunehud.demo_node')

# ── Plant simulation ──────────────────────────────────────────────────────────

class SecondOrderPlant:
    """
    Simulates a second-order system (e.g. motor velocity):
      G(s) = K / (tau1*s + 1)(tau2*s + 1)
    Discretised with forward Euler at DT seconds.
    """
    def __init__(self, K=1.0, tau1=0.2, tau2=0.05, dt=0.01):
        self.K    = K
        self.tau1 = tau1
        self.tau2 = tau2
        self.dt   = dt
        self.x1   = 0.0   # state 1
        self.x2   = 0.0   # state 2 (output)
        self.noise_std = 0.05

    def step(self, u: float) -> float:
        """Advance plant one time step with input u, return output."""
        dx1 = (-self.x1 + self.K * u) / self.tau1
        dx2 = (-self.x2 + self.x1) / self.tau2
        self.x1 += dx1 * self.dt
        self.x2 += dx2 * self.dt
        noise = random.gauss(0, self.noise_std)
        return self.x2 + noise

    def reset(self):
        self.x1 = 0.0
        self.x2 = 0.0


class PIDController:
    def __init__(self, kp=1.0, ki=0.1, kd=0.05, dt=0.01,
                 out_min=-1.0, out_max=1.0):
        self.kp = kp
        self.ki = ki
        self.kd = kd
        self.dt = dt
        self.out_min = out_min
        self.out_max = out_max
        self._integral  = 0.0
        self._prev_error = 0.0

    def update(self, setpoint: float, measurement: float) -> float:
        error = setpoint - measurement
        self._integral += error * self.dt
        # Anti-windup clamp
        self._integral = max(-10.0, min(10.0, self._integral))
        derivative = (error - self._prev_error) / self.dt
        self._prev_error = error

        output = (self.kp * error +
                  self.ki * self._integral +
                  self.kd * derivative)
        return max(self.out_min, min(self.out_max, output))

    def reset(self):
        self._integral = 0.0
        self._prev_error = 0.0


# ── Param definitions ─────────────────────────────────────────────────────────

MANIFEST = {
    'transport': 'websocket',
    'device':    'TuneHUD Demo Node — Motor Velocity Loop',
    'version':   '1.0',
    'params': [
        {'name':'kp',          'label':'Proportional gain', 'type':'float', 'units':None,  'min':0.0,   'max':20.0,  'step':0.01,  'read_only':False, 'group':'PID gains'},
        {'name':'ki',          'label':'Integral gain',     'type':'float', 'units':None,  'min':0.0,   'max':5.0,   'step':0.01,  'read_only':False, 'group':'PID gains'},
        {'name':'kd',          'label':'Derivative gain',   'type':'float', 'units':None,  'min':0.0,   'max':2.0,   'step':0.001, 'read_only':False, 'group':'PID gains'},
        {'name':'setpoint',    'label':'Setpoint',          'type':'float', 'units':'rpm', 'min':-100.0,'max':100.0, 'step':1.0,   'read_only':False, 'group':'Control'},
        {'name':'pv',          'label':'Process value',     'type':'float', 'units':'rpm', 'min':None,  'max':None,  'step':None,  'read_only':True,  'group':'Control'},
        {'name':'output',      'label':'Controller output', 'type':'float', 'units':None,  'min':-1.0,  'max':1.0,   'step':None,  'read_only':True,  'group':'Control'},
        {'name':'error',       'label':'Error',             'type':'float', 'units':'rpm', 'min':None,  'max':None,  'step':None,  'read_only':True,  'group':'Control'},
        {'name':'disturbance', 'label':'Disturbance',       'type':'float', 'units':'rpm', 'min':-20.0, 'max':20.0,  'step':1.0,   'read_only':False, 'group':'Test'},
        {'name':'noise_std',   'label':'Noise std dev',     'type':'float', 'units':'rpm', 'min':0.0,   'max':5.0,   'step':0.01,  'read_only':False, 'group':'Test'},
    ]
}


# ── Demo node server ──────────────────────────────────────────────────────────

class DemoNode:

    DT = 0.01  # simulation timestep (100 Hz)

    def __init__(self, port: int = 8766):
        self.port   = port
        self.plant  = SecondOrderPlant(K=50.0, tau1=0.15, tau2=0.04, dt=self.DT)
        self.pid    = PIDController(kp=1.0, ki=0.5, kd=0.05, dt=self.DT)
        self._clients: set[WebSocketServerProtocol] = set()

        # Tunable state
        self.setpoint    = 50.0
        self.disturbance = 0.0
        self.pv          = 0.0
        self.output      = 0.0
        self.error       = 0.0

    # ── Simulation loop ───────────────────────────────────────────────────────

    async def _sim_loop(self) -> None:
        """Run plant + PID at DT rate, broadcast stream_data to all clients."""
        log.info(f'Simulation loop started at {int(1/self.DT)} Hz')
        STREAM_HZ = 20  # stream to clients at 20 Hz
        stream_interval = 1.0 / STREAM_HZ
        last_stream = 0.0

        while True:
            t0 = time.time()

            self.output = self.pid.update(self.setpoint, self.pv)
            self.pv     = self.plant.step(self.output + self.disturbance / 50.0)
            self.error  = self.setpoint - self.pv

            now = time.time()
            if now - last_stream >= stream_interval and self._clients:
                msg = json.dumps({
                    'type': 'stream_data',
                    't':    now,
                    'data': {
                        'kp':          self.pid.kp,
                        'ki':          self.pid.ki,
                        'kd':          self.pid.kd,
                        'setpoint':    round(self.setpoint, 3),
                        'pv':          round(self.pv, 3),
                        'output':      round(self.output, 4),
                        'error':       round(self.error, 3),
                        'disturbance': self.disturbance,
                        'noise_std':   self.plant.noise_std,
                    }
                })
                dead = set()
                for ws in self._clients:
                    try:
                        await ws.send(msg)
                    except websockets.exceptions.ConnectionClosed:
                        dead.add(ws)
                self._clients -= dead
                last_stream = now

            elapsed = time.time() - t0
            sleep = self.DT - elapsed
            if sleep > 0:
                await asyncio.sleep(sleep)

    # ── WebSocket handler ─────────────────────────────────────────────────────

    async def _handle(self, ws: WebSocketServerProtocol) -> None:
        self._clients.add(ws)
        log.info(f'Client connected: {ws.remote_address}')
        try:
            async for raw in ws:
                try:
                    msg = json.loads(raw)
                except json.JSONDecodeError:
                    continue

                mtype = msg.get('type')

                if mtype == 'manifest_req':
                    await ws.send(json.dumps({'type': 'manifest_resp', **MANIFEST}))

                elif mtype == 'param_write':
                    name  = msg.get('name')
                    value = msg.get('value')
                    ack   = await self._write(name, value)
                    await ws.send(json.dumps({
                        'type':  'write_ack',
                        'name':  name,
                        'value': ack,
                        't':     time.time(),
                    }))

                elif mtype == 'ping':
                    await ws.send(json.dumps({'type': 'pong', 't': time.time()}))

        except websockets.exceptions.ConnectionClosed:
            pass
        finally:
            self._clients.discard(ws)
            log.info(f'Client disconnected: {ws.remote_address}')

    async def _write(self, name: str, value) -> float:
        v = float(value)
        if name == 'kp':
            self.pid.kp = max(0.0, v)
        elif name == 'ki':
            self.pid.ki = max(0.0, v)
            self.pid.reset()
        elif name == 'kd':
            self.pid.kd = max(0.0, v)
        elif name == 'setpoint':
            self.setpoint = max(-100.0, min(100.0, v))
        elif name == 'disturbance':
            self.disturbance = max(-20.0, min(20.0, v))
        elif name == 'noise_std':
            self.plant.noise_std = max(0.0, v)
        return v

    # ── Entry point ───────────────────────────────────────────────────────────

    async def run(self) -> None:
        log.info(f'TuneHUD Demo Node starting on ws://0.0.0.0:{self.port}')
        log.info(f'Simulating: second-order motor velocity loop')
        log.info(f'Initial gains: Kp={self.pid.kp} Ki={self.pid.ki} Kd={self.pid.kd}')
        async with websockets.serve(self._handle, '0.0.0.0', self.port):
            log.info(f'Listening on ws://0.0.0.0:{self.port}')
            await self._sim_loop()


def main():
    parser = argparse.ArgumentParser(description='TuneHUD Demo Node')
    parser.add_argument('--port',    type=int, default=8766)
    parser.add_argument('--verbose', action='store_true')
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    )

    node = DemoNode(port=args.port)
    try:
        asyncio.run(node.run())
    except KeyboardInterrupt:
        log.info('Demo node stopped')


if __name__ == '__main__':
    main()
