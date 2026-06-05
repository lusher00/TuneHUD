"""
tunehud_gateway/camera.py
MJPEG camera streamer for Raspberry Pi 5 using picamera2.
Runs as a separate asyncio task, serves MJPEG on /camera endpoint.

Install:
  sudo apt install -y python3-picamera2
  pip3 install av --break-system-packages  # optional, for H264
"""

import asyncio
import io
import logging
import sys
import os
import time
import threading
from typing import Optional, Set

# libcamera is installed as a system package but may not be in pip Python's path
_LIBCAMERA_PATHS = [
    '/usr/lib/aarch64-linux-gnu/python3.12/site-packages',
    '/usr/local/lib/python3/dist-packages',
]
for _p in _LIBCAMERA_PATHS:
    if _p not in sys.path and os.path.exists(_p):
        sys.path.insert(0, _p)

log = logging.getLogger('tunehud.camera')

try:
    from picamera2 import Picamera2
    from picamera2.encoders import JpegEncoder
    from picamera2.outputs import FileOutput
    HAS_PICAMERA2 = True
except ImportError:
    HAS_PICAMERA2 = False
    log.warning('picamera2 not available — camera streaming disabled')


class MJPEGOutput:
    """Thread-safe frame buffer for MJPEG streaming."""

    def __init__(self):
        self._frame: Optional[bytes] = None
        self._lock = threading.Lock()
        self._event = threading.Event()

    def write(self, data: bytes) -> None:
        with self._lock:
            self._frame = bytes(data)
        self._event.set()
        self._event.clear()

    def get_frame(self, timeout: float = 1.0) -> Optional[bytes]:
        self._event.wait(timeout)
        with self._lock:
            return self._frame


class CameraStreamer:
    """
    Captures frames from Pi Camera Module 3 and serves MJPEG.
    Integrates with the TuneHUD gateway HTTP handler.
    """

    def __init__(self, width: int = 1280, height: int = 720, fps: int = 15):
        self.width  = width
        self.height = height
        self.fps    = fps
        self._output: Optional[MJPEGOutput] = None
        self._camera: Optional[object] = None
        self._running = False
        self._clients: Set = set()

    def start(self) -> bool:
        if not HAS_PICAMERA2:
            log.warning('picamera2 not installed — camera disabled')
            return False
        try:
            self._camera = Picamera2()
            config = self._camera.create_video_configuration(
                main={'size': (self.width, self.height), 'format': 'RGB888'},
                controls={'FrameRate': self.fps},
            )
            self._camera.configure(config)
            self._output = MJPEGOutput()
            encoder = JpegEncoder(q=70)
            self._camera.start_recording(encoder, FileOutput(self._output))
            self._running = True
            log.info('Camera started: {}x{} @ {} fps'.format(
                self.width, self.height, self.fps))
            return True
        except Exception as e:
            log.error('Camera failed to start: {}'.format(e))
            return False

    def stop(self) -> None:
        if self._camera and self._running:
            try:
                self._camera.stop_recording()
            except Exception:
                pass
            self._running = False

    def is_running(self) -> bool:
        return self._running

    async def handle_mjpeg(self, send_response, send_data):
        """
        Called by the gateway HTTP handler for /camera requests.
        send_response(status, headers) and send_data(chunk) are callables.
        """
        if not self._running or not self._output:
            await send_response(503, [('Content-Type', 'text/plain')], b'Camera not available')
            return

        boundary = b'--TuneHUDframe'
        await send_response(200, [
            ('Content-Type', 'multipart/x-mixed-replace; boundary=TuneHUDframe'),
            ('Cache-Control', 'no-cache'),
            ('Pragma', 'no-cache'),
        ])

        try:
            while True:
                frame = self._output.get_frame(timeout=2.0)
                if frame is None:
                    continue
                header = (boundary + b'\r\nContent-Type: image/jpeg\r\n'
                          b'Content-Length: ' + str(len(frame)).encode() + b'\r\n\r\n')
                await send_data(header + frame + b'\r\n')
                await asyncio.sleep(1.0 / self.fps)
        except Exception:
            pass

    def get_snapshot_jpeg(self) -> Optional[bytes]:
        """Return the latest frame as JPEG bytes."""
        if not self._running or not self._output:
            return None
        return self._output.get_frame(timeout=1.0)
