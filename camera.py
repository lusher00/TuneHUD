"""
tunehud_gateway/camera.py
MJPEG camera streamer using rpicam-vid subprocess.
No picamera2 dependency — works on any Pi with libcamera installed.
"""

import asyncio
import logging
import subprocess
import threading
import time
from typing import Optional

log = logging.getLogger('tunehud.camera')

# Check rpicam-vid is available
import shutil
HAS_RPICAM = shutil.which('rpicam-vid') is not None or shutil.which('libcamera-vid') is not None

def _rpicam_cmd():
    if shutil.which('rpicam-vid'):
        return 'rpicam-vid'
    return 'libcamera-vid'


class CameraStreamer:
    """
    Captures MJPEG frames from Pi camera using rpicam-vid subprocess.
    Parses the MJPEG stream and serves latest frame via get_snapshot_jpeg().
    """

    def __init__(self, width=1280, height=720, fps=15):
        self.width  = width
        self.height = height
        self.fps    = fps
        self._proc: Optional[subprocess.Popen] = None
        self._frame: Optional[bytes] = None
        self._lock  = threading.Lock()
        self._thread: Optional[threading.Thread] = None
        self._running = False

    def start(self) -> bool:
        if not HAS_RPICAM:
            log.warning('rpicam-vid not found — camera disabled')
            return False
        try:
            cmd = [
                _rpicam_cmd(),
                '--codec',          'mjpeg',
                '--inline',
                '--nopreview',
                '--width',          str(self.width),
                '--height',         str(self.height),
                '--framerate',      str(self.fps),
                '--denoise',        'off',
                '--autofocus-mode', 'manual',
                '--flush',
                '--timeout',        '0',
                '--output',         '-',
            ]
            log.info('Starting camera: {}'.format(' '.join(cmd)))
            self._proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                bufsize=0,
            )
            self._running = True
            self._thread = threading.Thread(target=self._read_loop, daemon=True)
            self._thread.start()
            # Wait briefly for first frame
            time.sleep(2.0)
            if self._frame:
                log.info('Camera started: {}x{} @ {} fps'.format(
                    self.width, self.height, self.fps))
                return True
            else:
                log.warning('Camera started but no frames received yet')
                return True  # still return True, may just be slow to start
        except Exception as e:
            log.error('Camera failed to start: {}'.format(e))
            return False

    def _read_loop(self):
        """Parse MJPEG stream from rpicam-vid stdout."""
        SOI = b'\xff\xd8'  # JPEG start marker
        EOI = b'\xff\xd9'  # JPEG end marker
        buf = b''
        try:
            while self._running and self._proc:
                chunk = self._proc.stdout.read(16384)
                if not chunk:
                    break
                buf += chunk
                # Find complete JPEG frames
                while True:
                    start = buf.find(SOI)
                    if start == -1:
                        buf = b''
                        break
                    end = buf.find(EOI, start + 2)
                    if end == -1:
                        buf = buf[start:]  # keep partial frame
                        break
                    frame = buf[start:end + 2]
                    with self._lock:
                        self._frame = frame
                    buf = buf[end + 2:]
        except Exception as e:
            if self._running:
                log.warning('Camera read error: {}'.format(e))
        self._running = False

    def stop(self):
        self._running = False
        if self._proc:
            self._proc.terminate()
            try:
                self._proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                self._proc.kill()
            self._proc = None

    def is_running(self) -> bool:
        return self._running and self._frame is not None

    def get_snapshot_jpeg(self) -> Optional[bytes]:
        with self._lock:
            return self._frame
