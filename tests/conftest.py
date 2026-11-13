import os
import socket
import subprocess
import sys
import time

import httpx
import pytest


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class Server:
    """以独立子进程运行 uvicorn，停止/重启等同于真实进程重启。"""

    _log_dir = "/tmp/pytest-uvicorn-logs"

    def __init__(self, app: str, env: dict[str, str]):
        self.app = app
        self.env = env
        self.port = _free_port()
        self.base_url = f"http://127.0.0.1:{self.port}"
        self.proc: subprocess.Popen | None = None
        os.makedirs(self._log_dir, exist_ok=True)
        safe = app.replace(":", "_").replace(".", "_")
        self.log_path = os.path.join(self._log_dir, f"{safe}_{self.port}.log")

    def start(self):
        env = {**os.environ, **self.env}
        # 日志写文件而非 PIPE：子进程大量输出时不会因管道缓冲填满而死锁
        log_fh = open(self.log_path, "ab", buffering=0)
        self.proc = subprocess.Popen(
            [
                sys.executable, "-I", "-m", "uvicorn",
                self.app, "--app-dir", "/workspace",
                "--host", "127.0.0.1", "--port", str(self.port),
                "--log-level", "warning",
            ],
            env=env,
            stdout=log_fh,
            stderr=subprocess.STDOUT,
        )
        self._log_fh = log_fh
        deadline = time.time() + 30
        while time.time() < deadline:
            if self.proc.poll() is not None:
                with open(self.log_path, "rb") as fh:
                    out = fh.read().decode(errors="replace")
                raise RuntimeError(f"服务进程提前退出:\n{out}")
            try:
                if httpx.get(f"{self.base_url}/healthz", timeout=1).status_code == 200:
                    return
            except httpx.HTTPError:
                time.sleep(0.15)
        self.stop()
        raise RuntimeError("测试服务启动超时")

    def stop(self):
        if self.proc is None:
            return
        self.proc.terminate()
        try:
            self.proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait(timeout=5)
        self.proc = None
        fh = getattr(self, "_log_fh", None)
        if fh is not None:
            fh.close()
            self._log_fh = None

    def restart(self, new_port: bool = False):
        self.stop()
        if new_port:
            self.port = _free_port()
            self.base_url = f"http://127.0.0.1:{self.port}"
        self.start()


@pytest.fixture(scope="session")
def servers(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("svc")
    self_relay = Server("relay.app:app", {"RELAY_DATA": str(tmp / "relay")})
    self_relay.start()
    db_path = str(tmp / "collector.db")
    collector = Server("collector.app:app", {"COLLECTOR_DB": db_path})
    collector.start()
    yield self_relay.base_url, collector.base_url, {"db_path": db_path,
                                                    "relay_data": str(tmp / "relay")}
    collector.stop()
    self_relay.stop()


@pytest.fixture()
def client():
    with httpx.Client(timeout=30) as c:
        yield c
