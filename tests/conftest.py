import os
import sys
import json
import time
import signal
import shutil
import socket
import subprocess
import tempfile
import threading
from pathlib import Path

import pytest
import requests


# 项目根目录
PROJECT_ROOT = Path(__file__).resolve().parent.parent
BUILD_DIR = PROJECT_ROOT / "build"


def find_free_port():
    """找一个空闲端口"""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]


def build_server():
    """编译 my-etcd 服务端"""
    build_dir = BUILD_DIR
    build_dir.mkdir(exist_ok=True)

    # CMake 配置
    result = subprocess.run(
        ["cmake", "..", "-G", "Visual Studio 17 2022", "-A", "x64"],
        cwd=str(build_dir),
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print(f"[BUILD] CMake configure failed:\n{result.stderr}")
        return False

    # CMake 构建
    result = subprocess.run(
        ["cmake", "--build", ".", "--config", "Debug"],
        cwd=str(build_dir),
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print(f"[BUILD] CMake build failed:\n{result.stderr}")
        return False

    print("[BUILD] Build succeeded")
    return True


def find_server_exe():
    """查找编译好的服务端可执行文件"""
    candidates = [
        BUILD_DIR / "Debug" / "my-etcd.exe",
        BUILD_DIR / "my-etcd.exe",
    ]
    for c in candidates:
        if c.exists():
            return str(c)
    return None


class EtcdServerProcess:
    """管理 my-etcd 服务端进程"""

    def __init__(self, port=None, peer_port=None):
        self.port = port or find_free_port()
        self.peer_port = peer_port or find_free_port()
        self.data_dir = tempfile.mkdtemp(prefix="myetcd_test_")
        self.process = None
        self.base_url = f"http://localhost:{self.port}"

    def start(self):
        exe = find_server_exe()
        if not exe:
            if not build_server():
                raise RuntimeError("Failed to build my-etcd server")
            exe = find_server_exe()
            if not exe:
                raise RuntimeError("my-etcd.exe not found after build")

        cmd = [
            exe,
            "--name", "test-node",
            "--data-dir", self.data_dir,
            "--listen-addr", f"0.0.0.0:{self.port}",
            "--listen-peer-addr", f"0.0.0.0:{self.peer_port}",
        ]

        self.process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
        )

        # 启动输出收集线程
        self.output_lines = []
        self._reader_thread = threading.Thread(
            target=self._read_output, daemon=True
        )
        self._reader_thread.start()

        # 等待服务启动
        self._wait_ready(timeout=15)

    def _read_output(self):
        """读取服务端输出"""
        if self.process and self.process.stdout:
            for line in self.process.stdout:
                self.output_lines.append(line.rstrip())

    def _wait_ready(self, timeout=15):
        """等待服务就绪"""
        deadline = time.time() + timeout
        last_error = None
        while time.time() < deadline:
            try:
                resp = requests.get(
                    f"{self.base_url}/v3/cluster/info", timeout=2
                )
                if resp.status_code == 200:
                    # 等待成为 Leader
                    data = resp.json()
                    if data.get("state") == "Leader":
                        return True
            except requests.ConnectionError as e:
                last_error = e
            except Exception:
                pass
            time.sleep(0.3)

        # 打印输出以便调试
        for line in self.output_lines[-20:]:
            print(f"  [SERVER] {line}")

        raise RuntimeError(
            f"Server failed to start within {timeout}s: {last_error}"
        )

    def stop(self):
        """停止服务端"""
        if self.process:
            try:
                self.process.terminate()
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)
            except Exception:
                pass
            self.process = None

        # 清理数据目录
        if os.path.exists(self.data_dir):
            shutil.rmtree(self.data_dir, ignore_errors=True)

    def get(self, path, **kwargs):
        """HTTP GET 请求"""
        return requests.get(f"{self.base_url}{path}", timeout=5, **kwargs)

    def post(self, path, **kwargs):
        """HTTP POST 请求"""
        return requests.post(f"{self.base_url}{path}", timeout=5, **kwargs)

    def put(self, path, **kwargs):
        """HTTP PUT 请求"""
        return requests.put(f"{self.base_url}{path}", timeout=5, **kwargs)


# ============================================================
# Pytest Fixtures
# ============================================================

@pytest.fixture(scope="session")
def server():
    """会话级别的服务端 fixture"""
    srv = EtcdServerProcess()
    srv.start()
    yield srv
    srv.stop()


@pytest.fixture(scope="function")
def kv(server):
    """每个测试用例独立的 KV 辅助类"""
    class KVHelper:
        @staticmethod
        def put(key, value):
            resp = server.put(f"/v3/kv/put?key={key}", data=value)
            return resp.json()

        @staticmethod
        def get(key):
            resp = server.post(f"/v3/kv/range?key={key}")
            data = resp.json()
            kvs = data.get("kvs", [])
            return kvs[0] if kvs else None

        @staticmethod
        def range(start, end=""):
            params = f"?key={start}"
            if end:
                params += f"&range_end={end}"
            resp = server.post(f"/v3/kv/range{params}")
            return resp.json()

        @staticmethod
        def delete(key):
            resp = server.post(f"/v3/kv/delete?key={key}")
            return resp.json()

        @staticmethod
        def get_all():
            resp = server.post("/v3/kv/range?key=\x00&range_end=\xff")
            return resp.json()

    return KVHelper()


@pytest.fixture(scope="function")
def clean_kv(server):
    """清理所有 KV 数据后的辅助类"""
    # 获取所有 key 并删除
    try:
        resp = server.post("/v3/kv/range?key=a&range_end=zzzzzzzzzzzzzzzzzzzzz")
        data = resp.json()
        for kv_item in data.get("kvs", []):
            server.post(f"/v3/kv/delete?key={kv_item['key']}")
    except Exception:
        pass

    class KVHelper:
        def __init__(self, srv):
            self.server = srv

        def put(self, key, value):
            resp = self.server.put(f"/v3/kv/put?key={key}", data=value)
            return resp.json()

        def get(self, key):
            resp = self.server.post(f"/v3/kv/range?key={key}")
            data = resp.json()
            kvs = data.get("kvs", [])
            return kvs[0] if kvs else None

        def range(self, start, end=""):
            params = f"?key={start}"
            if end:
                params += f"&range_end={end}"
            resp = self.server.post(f"/v3/kv/range{params}")
            return resp.json()

        def delete(self, key):
            resp = self.server.post(f"/v3/kv/delete?key={key}")
            return resp.json()

    return KVHelper(server)