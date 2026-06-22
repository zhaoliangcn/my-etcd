#!/usr/bin/env python3
"""
my-etcd 自动化测试运行器

用法:
    python run_tests.py              # 运行所有测试
    python run_tests.py --quick      # 快速冒烟测试
    python run_tests.py --kv         # 仅 KV 测试
    python run_tests.py --lease      # 仅 Lease 测试
    python run_tests.py --watch      # 仅 Watch 测试
    python run_tests.py --cluster    # 仅 Cluster 测试
"""

import os
import sys
import json
import time
import shutil
import socket
import signal
import atexit
import tempfile
import subprocess
import threading
import traceback
from pathlib import Path

# ============================================================
# 工具函数
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parent.parent
BUILD_DIR = PROJECT_ROOT / "build"
COLORS = {
    "green": "\033[92m",
    "red": "\033[91m",
    "yellow": "\033[93m",
    "blue": "\033[94m",
    "reset": "\033[0m",
    "bold": "\033[1m",
}


def colorize(text, color):
    if os.name == "nt":
        return text  # Windows 终端可能不支持
    return f"{COLORS.get(color, '')}{text}{COLORS['reset']}"


def find_free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]


def http_get(url, timeout=5):
    """简单的 HTTP GET 请求"""
    import http.client
    import urllib.parse
    try:
        parsed = urllib.parse.urlparse(url)
        host = parsed.hostname
        port = parsed.port or 80
        path = parsed.path
        if parsed.query:
            path += "?" + parsed.query
        conn = http.client.HTTPConnection(host, port, timeout=timeout)
        conn.request("GET", path)
        resp = conn.getresponse()
        resp_body = resp.read().decode("utf-8", errors="replace")
        status = resp.status
        headers = dict(resp.getheaders())
        conn.close()
        return status, resp_body, headers
    except Exception as e:
        return None, str(e), {}


def http_post(url, body="", timeout=5):
    """简单的 HTTP POST 请求"""
    import http.client
    import urllib.parse
    try:
        parsed = urllib.parse.urlparse(url)
        host = parsed.hostname
        port = parsed.port or 80
        path = parsed.path
        if parsed.query:
            path += "?" + parsed.query
        data = body.encode("utf-8") if body else b""
        conn = http.client.HTTPConnection(host, port, timeout=timeout)
        conn.request("POST", path, body=data, headers={"Content-Type": "application/octet-stream"})
        resp = conn.getresponse()
        resp_body = resp.read().decode("utf-8", errors="replace")
        status = resp.status
        headers = dict(resp.getheaders())
        conn.close()
        return status, resp_body, headers
    except Exception as e:
        return None, str(e), {}


def http_put(url, body="", timeout=5):
    """简单的 HTTP PUT 请求"""
    import http.client
    import urllib.parse
    try:
        parsed = urllib.parse.urlparse(url)
        host = parsed.hostname
        port = parsed.port or 80
        path = parsed.path
        if parsed.query:
            path += "?" + parsed.query
        data = body.encode("utf-8") if body else b""
        conn = http.client.HTTPConnection(host, port, timeout=timeout)
        conn.request("PUT", path, body=data, headers={
            "Content-Type": "application/octet-stream",
        })
        resp = conn.getresponse()
        resp_body = resp.read().decode("utf-8", errors="replace")
        status = resp.status
        headers = dict(resp.getheaders())
        conn.close()
        return status, resp_body, headers
    except ConnectionResetError as e:
        return None, str(e), {}
    except Exception as e:
        return None, str(e), {}


def url_encode(s):
    """URL 编码（用于 query 参数中的中文等特殊字符）"""
    import urllib.parse
    return urllib.parse.quote(s, safe="")


# ============================================================
# 测试结果收集
# ============================================================

class TestResult:
    def __init__(self):
        self.passed = 0
        self.failed = 0
        self.errors = 0
        self.skipped = 0
        self.results = []

    def add_pass(self, name, duration=0):
        self.passed += 1
        self.results.append(("PASS", name, duration, ""))

    def add_fail(self, name, msg, duration=0):
        self.failed += 1
        self.results.append(("FAIL", name, duration, msg))

    def add_error(self, name, msg, duration=0):
        self.errors += 1
        self.results.append(("ERROR", name, duration, msg))

    def summary(self):
        total = self.passed + self.failed + self.errors
        print()
        print("=" * 60)
        print(f"  Results: {colorize(str(self.passed) + ' passed', 'green')}, "
              f"{colorize(str(self.failed) + ' failed', 'red')}, "
              f"{colorize(str(self.errors) + ' errors', 'yellow')}")
        print(f"  Total:   {total} tests")
        print("=" * 60)

        for status, name, duration, msg in self.results:
            if status == "PASS":
                print(f"  {colorize('[PASS]', 'green')} {name} ({duration:.2f}s)")
            elif status == "FAIL":
                print(f"  {colorize('[FAIL]', 'red')} {name} ({duration:.2f}s)")
                if msg:
                    for line in msg.split("\n")[:5]:
                        if line.strip():
                            print(f"         {line.strip()}")
            elif status == "ERROR":
                print(f"  {colorize('[ERROR]', 'yellow')} {name} ({duration:.2f}s)")
                if msg:
                    for line in msg.split("\n")[:5]:
                        if line.strip():
                            print(f"         {line.strip()}")

        return self.failed == 0 and self.errors == 0


# ============================================================
# 测试用例
# ============================================================

class TestRunner:
    def __init__(self, base_url):
        self.url = base_url
        self.result = TestResult()

    def run_test(self, name, func, *args, **kwargs):
        start = time.time()
        try:
            func(*args, **kwargs)
            elapsed = time.time() - start
            self.result.add_pass(name, elapsed)
            print(f"  {colorize('[PASS]', 'green')} {name}")
        except AssertionError as e:
            elapsed = time.time() - start
            self.result.add_fail(name, str(e), elapsed)
            print(f"  {colorize('[FAIL]', 'red')} {name}: {e}")
        except Exception as e:
            elapsed = time.time() - start
            self.result.add_error(name, traceback.format_exc(), elapsed)
            print(f"  {colorize('[ERROR]', 'yellow')} {name}: {e}")

    def assert_equal(self, a, b, msg=""):
        assert a == b, f"Expected {b!r}, got {a!r}. {msg}"

    def assert_true(self, cond, msg=""):
        assert cond, msg

    def assert_in(self, item, container, msg=""):
        assert item in container, f"{item!r} not in {container!r}. {msg}"

    def assert_greater(self, a, b, msg=""):
        assert a > b, f"Expected {a!r} > {b!r}. {msg}"

    # ---- KV 测试 ----

    def test_kv_put_get(self):
        status, put_body, _ = http_put(f"{self.url}/v3/kv/put?key=hello", "world")
        self.assert_equal(status, 200)
        data = json.loads(put_body)
        self.assert_true(data.get("succeeded"), "PUT should succeed")

        # 等待 Raft 提交
        time.sleep(0.3)

        status, body, _ = http_post(f"{self.url}/v3/kv/range?key=hello")
        self.assert_equal(status, 200)
        data = json.loads(body)
        kvs = data.get("kvs", [])
        self.assert_equal(len(kvs), 1, f"body={body}")
        self.assert_equal(kvs[0]["value"], "world")

    def test_kv_put_overwrite(self):
        http_put(f"{self.url}/v3/kv/put?key=overwrite", "old")
        http_put(f"{self.url}/v3/kv/put?key=overwrite", "new")
        time.sleep(0.3)

        _, body, _ = http_post(f"{self.url}/v3/kv/range?key=overwrite")
        kvs = json.loads(body).get("kvs", [])
        self.assert_equal(len(kvs), 1, f"body={body}")
        self.assert_equal(kvs[0]["value"], "new")
        self.assert_greater(kvs[0]["version"], 1)

    def test_kv_delete(self):
        http_put(f"{self.url}/v3/kv/put?key=to_delete", "val")
        time.sleep(0.3)
        status, body, _ = http_post(f"{self.url}/v3/kv/delete?key=to_delete")
        self.assert_equal(status, 200)
        data = json.loads(body)
        self.assert_equal(data.get("deleted"), 1)

        time.sleep(0.3)
        _, body, _ = http_post(f"{self.url}/v3/kv/range?key=to_delete")
        kvs = json.loads(body).get("kvs", [])
        self.assert_equal(len(kvs), 0)

    def test_kv_get_nonexistent(self):
        _, body, _ = http_post(f"{self.url}/v3/kv/range?key=nonexistent_xyz")
        kvs = json.loads(body).get("kvs", [])
        self.assert_equal(len(kvs), 0)

    def test_kv_range(self):
        for i in range(5):
            http_put(f"{self.url}/v3/kv/put?key=range_{i}", f"val_{i}")
        time.sleep(0.3)

        _, body, _ = http_post(f"{self.url}/v3/kv/range?key=range_0&range_end=range_5")
        kvs = json.loads(body).get("kvs", [])
        self.assert_equal(len(kvs), 5, f"body={body}")

    def test_kv_unicode(self):
        encoded_key = url_encode('中文键')
        status, put_body, _ = http_put(f"{self.url}/v3/kv/put?key={encoded_key}", "中文值")
        time.sleep(0.3)
        _, body, _ = http_post(f"{self.url}/v3/kv/range?key={encoded_key}")
        kvs = json.loads(body).get("kvs", [])
        self.assert_equal(len(kvs), 1, f"body={body}")
        self.assert_equal(kvs[0]["value"], "中文值")

    def test_kv_special_chars(self):
        status, put_body, _ = http_put(f"{self.url}/v3/kv/put?key=key%2Fwith%2Fslash", '{"json":true}')
        time.sleep(0.3)
        _, body, _ = http_post(f"{self.url}/v3/kv/range?key=key%2Fwith%2Fslash")
        kvs = json.loads(body).get("kvs", [])
        self.assert_equal(len(kvs), 1, f"body={body}")
        self.assert_equal(kvs[0]["value"], '{"json":true}')

    def test_kv_revision_monotonic(self):
        revs = []
        for i in range(20):
            _, body, _ = http_put(f"{self.url}/v3/kv/put?key=rev_{i}", str(i))
            rev = json.loads(body)["header"]["revision"]
            revs.append(rev)
            time.sleep(0.05)
        self.assert_equal(revs, sorted(revs))
        self.assert_equal(len(revs), len(set(revs)))

    def test_kv_many_keys(self):
        for i in range(50):
            http_put(f"{self.url}/v3/kv/put?key=bulk_{i}", f"data_{i}")
        time.sleep(0.5)
        for i in range(50):
            _, body, _ = http_post(f"{self.url}/v3/kv/range?key=bulk_{i}")
            kvs = json.loads(body).get("kvs", [])
            self.assert_equal(len(kvs), 1, f"key=bulk_{i} body={body}")

    # ---- Lease 测试 ----

    def test_lease_grant(self):
        status, body, _ = http_post(f"{self.url}/v3/lease/grant?TTL=10")
        self.assert_equal(status, 200)
        data = json.loads(body)
        self.assert_greater(data["ID"], 0)
        self.assert_equal(data["TTL"], 10)

    def test_lease_grant_invalid(self):
        status, _, _ = http_post(f"{self.url}/v3/lease/grant?TTL=0")
        self.assert_equal(status, 400)
        status, _, _ = http_post(f"{self.url}/v3/lease/grant?TTL=-1")
        self.assert_equal(status, 400)

    def test_lease_revoke(self):
        _, body, _ = http_post(f"{self.url}/v3/lease/grant?TTL=60")
        lease_id = json.loads(body)["ID"]

        status, body, _ = http_post(f"{self.url}/v3/lease/revoke?ID={lease_id}")
        self.assert_equal(status, 200)
        self.assert_true(json.loads(body).get("revoked"))

    def test_lease_revoke_nonexistent(self):
        status, _, _ = http_post(f"{self.url}/v3/lease/revoke?ID=99999")
        self.assert_equal(status, 404)

    def test_lease_keepalive(self):
        _, body, _ = http_post(f"{self.url}/v3/lease/grant?TTL=60")
        lease_id = json.loads(body)["ID"]

        status, _, _ = http_post(f"{self.url}/v3/lease/keepalive?ID={lease_id}")
        self.assert_equal(status, 200)

    def test_lease_multiple(self):
        ids = set()
        for ttl in [5, 10, 30, 60]:
            _, body, _ = http_post(f"{self.url}/v3/lease/grant?TTL={ttl}")
            ids.add(json.loads(body)["ID"])
        self.assert_equal(len(ids), 4)

    # ---- Watch 测试 ----

    def test_watch_key_put(self):
        events = []

        def watch_thread():
            status, body, _ = http_post(
                f"{self.url}/v3/watch?key=watch_test", timeout=10
            )
            if status == 200:
                events.extend(json.loads(body).get("events", []))

        t = threading.Thread(target=watch_thread, daemon=True)
        t.start()
        time.sleep(0.5)

        http_put(f"{self.url}/v3/kv/put?key=watch_test", "hello")
        t.join(timeout=5)

        self.assert_greater(len(events), 0, "Should receive at least one event")

    def test_watch_prefix(self):
        events = []

        def watch_thread():
            status, body, _ = http_post(
                f"{self.url}/v3/watch?key=/wp/&prefix=true", timeout=10
            )
            if status == 200:
                events.extend(json.loads(body).get("events", []))

        t = threading.Thread(target=watch_thread, daemon=True)
        t.start()
        time.sleep(0.5)

        http_put(f"{self.url}/v3/kv/put?key=/wp/a", "1")
        http_put(f"{self.url}/v3/kv/put?key=/wp/b", "2")
        t.join(timeout=5)

        self.assert_greater(len(events), 0)

    # ---- Cluster 测试 ----

    def test_cluster_info(self):
        status, body, headers = http_get(f"{self.url}/v3/cluster/info")
        self.assert_equal(status, 200)
        data = json.loads(body)
        self.assert_equal(data["node_id"], 1)
        self.assert_in(data["state"], ["Leader", "Follower", "Candidate"])
        self.assert_greater(data["term"], -1)

    def test_root_endpoint(self):
        status, body, headers = http_get(f"{self.url}/")
        self.assert_equal(status, 200)
        self.assert_in("my-etcd", body)

    # ---- 错误处理测试 ----

    def test_error_404(self):
        status, _, _ = http_get(f"{self.url}/v3/nonexistent")
        self.assert_equal(status, 404)

    def test_error_put_missing_key(self):
        status, _, _ = http_put(f"{self.url}/v3/kv/put")
        self.assert_equal(status, 400)

    def test_error_range_missing_key(self):
        status, _, _ = http_post(f"{self.url}/v3/kv/range")
        self.assert_equal(status, 400)

    def test_error_delete_missing_key(self):
        status, _, _ = http_post(f"{self.url}/v3/kv/delete")
        self.assert_equal(status, 400)

    # ---- 服务器头测试 ----

    def test_server_header(self):
        _, _, headers = http_get(f"{self.url}/v3/cluster/info")
        self.assert_in("Server", headers)
        self.assert_in("my-etcd", headers.get("Server", ""))


# ============================================================
# 服务端管理
# ============================================================

class ServerManager:
    def __init__(self):
        self.port = find_free_port()
        self.peer_port = find_free_port()
        self.data_dir = tempfile.mkdtemp(prefix="myetcd_test_")
        self.process = None
        self.base_url = f"http://localhost:{self.port}"

    def build(self):
        build_dir = BUILD_DIR
        build_dir.mkdir(exist_ok=True)

        print("Building my-etcd...")
        result = subprocess.run(
            ["cmake", "..", "-G", "Visual Studio 17 2022", "-A", "x64"],
            cwd=str(build_dir),
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            print(f"CMake configure failed:\n{result.stderr}")
            return False

        result = subprocess.run(
            ["cmake", "--build", ".", "--config", "Debug"],
            cwd=str(build_dir),
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            print(f"CMake build failed:\n{result.stderr}")
            return False

        print("Build succeeded")
        return True

    def find_exe(self):
        for c in [BUILD_DIR / "Release" / "my-etcd.exe", BUILD_DIR / "Debug" / "my-etcd.exe", BUILD_DIR / "my-etcd.exe"]:
            if c.exists():
                return str(c)
        return None

    def start(self):
        exe = self.find_exe()
        if not exe:
            if not self.build():
                raise RuntimeError("Build failed")
            exe = self.find_exe()
            if not exe:
                raise RuntimeError("my-etcd.exe not found")

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

        self._output_lines = []

        def read_output():
            if self.process and self.process.stdout:
                for line in self.process.stdout:
                    self._output_lines.append(line.rstrip())

        self._reader = threading.Thread(target=read_output, daemon=True)
        self._reader.start()

        self._wait_ready()

    def _wait_ready(self, timeout=15):
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                status, body, _ = http_get(f"{self.base_url}/v3/cluster/info")
                if status == 200:
                    data = json.loads(body)
                    if data.get("state") == "Leader":
                        return
            except Exception:
                pass
            time.sleep(0.3)

        for line in self._output_lines[-20:]:
            print(f"  [SERVER] {line}")
        raise RuntimeError(f"Server failed to start within {timeout}s")

    def stop(self):
        if self.process:
            try:
                self.process.terminate()
                self.process.wait(timeout=5)
            except Exception:
                try:
                    self.process.kill()
                except Exception:
                    pass
            self.process = None
        if os.path.exists(self.data_dir):
            shutil.rmtree(self.data_dir, ignore_errors=True)


# ============================================================
# 主入口
# ============================================================

def main():
    import argparse

    parser = argparse.ArgumentParser(description="my-etcd test runner")
    parser.add_argument("--quick", action="store_true", help="Run quick smoke tests only")
    parser.add_argument("--kv", action="store_true", help="Run KV tests only")
    parser.add_argument("--lease", action="store_true", help="Run lease tests only")
    parser.add_argument("--watch", action="store_true", help="Run watch tests only")
    parser.add_argument("--cluster", action="store_true", help="Run cluster tests only")
    args = parser.parse_args()

    # 管理服务端
    srv = ServerManager()
    atexit.register(srv.stop)

    try:
        print("Starting my-etcd server...")
        srv.start()
        print(f"Server running at {srv.base_url}")
        print()
    except Exception as e:
        print(f"Failed to start server: {e}")
        return 1

    runner = TestRunner(srv.base_url)

    # 测试分组
    kv_tests = [
        ("KV Put/Get", runner.test_kv_put_get),
        ("KV Put Overwrite", runner.test_kv_put_overwrite),
        ("KV Delete", runner.test_kv_delete),
        ("KV Get Nonexistent", runner.test_kv_get_nonexistent),
        ("KV Range", runner.test_kv_range),
        ("KV Unicode", runner.test_kv_unicode),
        ("KV Special Chars", runner.test_kv_special_chars),
        ("KV Revision Monotonic", runner.test_kv_revision_monotonic),
        ("KV Many Keys", runner.test_kv_many_keys),
    ]

    lease_tests = [
        ("Lease Grant", runner.test_lease_grant),
        ("Lease Grant Invalid", runner.test_lease_grant_invalid),
        ("Lease Revoke", runner.test_lease_revoke),
        ("Lease Revoke Nonexistent", runner.test_lease_revoke_nonexistent),
        ("Lease Keepalive", runner.test_lease_keepalive),
        ("Lease Multiple", runner.test_lease_multiple),
    ]

    watch_tests = [
        ("Watch Key Put", runner.test_watch_key_put),
        ("Watch Prefix", runner.test_watch_prefix),
    ]

    cluster_tests = [
        ("Cluster Info", runner.test_cluster_info),
        ("Root Endpoint", runner.test_root_endpoint),
        ("Error 404", runner.test_error_404),
        ("Error PUT Missing Key", runner.test_error_put_missing_key),
        ("Error Range Missing Key", runner.test_error_range_missing_key),
        ("Error Delete Missing Key", runner.test_error_delete_missing_key),
        ("Server Header", runner.test_server_header),
    ]

    # 快速冒烟测试
    quick_tests = [
        kv_tests[0],
        kv_tests[2],
        kv_tests[3],
        lease_tests[0],
        cluster_tests[0],
        cluster_tests[2],
    ]

    # 选择测试集
    if args.quick:
        test_suite = quick_tests
        print("Running QUICK smoke tests...")
    elif args.kv:
        test_suite = kv_tests
        print("Running KV tests...")
    elif args.lease:
        test_suite = lease_tests
        print("Running Lease tests...")
    elif args.watch:
        test_suite = watch_tests
        print("Running Watch tests...")
    elif args.cluster:
        test_suite = cluster_tests
        print("Running Cluster tests...")
    else:
        test_suite = kv_tests + lease_tests + watch_tests + cluster_tests
        print("Running ALL tests...")

    print()
    for name, func in test_suite:
        runner.run_test(name, func)

    return 0 if runner.result.summary() else 1


if __name__ == "__main__":
    sys.exit(main())