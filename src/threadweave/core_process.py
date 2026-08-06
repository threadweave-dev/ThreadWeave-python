from __future__ import annotations

import json
import selectors
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import IO, Any


class CoreProcessError(RuntimeError):
    """Raised when the Rust Core cannot be started or stopped cleanly."""


@dataclass(frozen=True, slots=True)
class CoreEndpoint:
    address: str
    transport: str
    protocol: str


def decode_ready_message(line: str) -> CoreEndpoint:
    try:
        message: Any = json.loads(line)
    except json.JSONDecodeError as error:
        raise CoreProcessError("Core emitted invalid ready JSON") from error
    if not isinstance(message, dict):
        raise CoreProcessError("Core ready message must be a JSON object")
    if message.get("type") != "ready":
        raise CoreProcessError("Core message type is not 'ready'")
    endpoint = message.get("endpoint")
    if not isinstance(endpoint, str) or not endpoint:
        raise CoreProcessError("Core ready message is missing an endpoint")
    if message.get("transport") != "tcp" or message.get("protocol") != "grpc":
        raise CoreProcessError("Core ready message has an unsupported transport")
    return CoreEndpoint(endpoint, "tcp", "grpc")


class CoreProcess:
    def __init__(self, executable: str | Path, *, ready_timeout: float = 10.0) -> None:
        self._executable = Path(executable)
        self._ready_timeout = ready_timeout
        self._process: subprocess.Popen[str] | None = None
        self.endpoint: CoreEndpoint | None = None

    def start(self) -> CoreEndpoint:
        if self._process is not None:
            raise CoreProcessError("Core process is already started")
        try:
            process = subprocess.Popen(
                [str(self._executable)],
                stdout=subprocess.PIPE,
                text=True,
            )
        except FileNotFoundError as error:
            raise CoreProcessError(
                f"Core executable not found: {self._executable}"
            ) from error
        self._process = process
        assert process.stdout is not None
        line = self._readline(process.stdout)
        if not line:
            return_code = process.poll()
            self.stop()
            raise CoreProcessError(
                f"Core exited before ready message (status {return_code})"
            )
        try:
            self.endpoint = decode_ready_message(line)
        except Exception:
            self.stop()
            raise
        return self.endpoint

    def _readline(self, stdout: IO[str]) -> str:
        selector = selectors.DefaultSelector()
        selector.register(stdout, selectors.EVENT_READ)
        deadline = time.monotonic() + self._ready_timeout
        try:
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    self.stop()
                    raise CoreProcessError("Timed out waiting for Core ready message")
                if selector.select(remaining):
                    return stdout.readline()
                if self._process is not None and self._process.poll() is not None:
                    return ""
        finally:
            selector.close()

    def stop(self) -> None:
        process, self._process = self._process, None
        self.endpoint = None
        if process is None:
            return
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
        if process.stdout is not None:
            process.stdout.close()

    def __enter__(self) -> CoreProcess:
        self.start()
        return self

    def __exit__(self, *_args: object) -> None:
        self.stop()
