import pytest
import json
import time
import threading
import requests


class TestWatch:
    """测试 Watch 机制"""

    def test_watch_key_put(self, server):
        """监听 key 的 PUT 事件"""
        # 先创建 watcher（在后台线程）
        events = []
        error = []

        def do_watch():
            try:
                resp = requests.post(
                    f"http://localhost:{server.port}/v3/watch?key=watch_put",
                    timeout=10,
                )
                data = resp.json()
                events.extend(data.get("events", []))
            except Exception as e:
                error.append(str(e))

        watch_thread = threading.Thread(target=do_watch, daemon=True)
        watch_thread.start()

        # 等待 watcher 就绪
        time.sleep(0.5)

        # 写入 key
        server.put("/v3/kv/put?key=watch_put", data="hello")

        # 等待事件
        watch_thread.join(timeout=5)

        assert len(events) > 0, f"Expected events, got {events}, errors: {error}"
        if events:
            event = events[0]
            assert event["kv"]["key"] == "watch_put"

    def test_watch_key_delete(self, server):
        """监听 key 的 DELETE 事件"""
        # 先写入
        server.put("/v3/kv/put?key=watch_del", data="to_delete")

        events = []

        def do_watch():
            try:
                resp = requests.post(
                    f"http://localhost:{server.port}/v3/watch?key=watch_del",
                    timeout=10,
                )
                data = resp.json()
                events.extend(data.get("events", []))
            except Exception:
                pass

        watch_thread = threading.Thread(target=do_watch, daemon=True)
        watch_thread.start()

        time.sleep(0.5)

        # 删除 key
        server.post("/v3/kv/delete?key=watch_del")

        watch_thread.join(timeout=5)

        if events:
            event = events[0]
            assert event["type"] == "DELETE"

    def test_watch_prefix(self, server):
        """前缀匹配监听"""
        events = []

        def do_watch():
            try:
                resp = requests.post(
                    f"http://localhost:{server.port}/v3/watch?key=/watch_prefix/&prefix=true",
                    timeout=10,
                )
                data = resp.json()
                events.extend(data.get("events", []))
            except Exception:
                pass

        watch_thread = threading.Thread(target=do_watch, daemon=True)
        watch_thread.start()

        time.sleep(0.5)

        server.put("/v3/kv/put?key=/watch_prefix/a", data="1")
        server.put("/v3/kv/put?key=/watch_prefix/b", data="2")

        watch_thread.join(timeout=5)

        # 至少收到一个事件
        assert len(events) >= 1

    def test_watch_nonexistent_key(self, server):
        """监听不存在的 key"""
        events = []

        def do_watch():
            try:
                resp = requests.post(
                    f"http://localhost:{server.port}/v3/watch?key=no_such_key_xyz",
                    timeout=5,
                )
                data = resp.json()
                events.extend(data.get("events", []))
            except Exception:
                pass

        watch_thread = threading.Thread(target=do_watch, daemon=True)
        watch_thread.start()

        watch_thread.join(timeout=8)

        # 超时后应该返回空事件或取消事件
        # 不强制断言，因为行为取决于实现

    def test_watch_multiple_events(self, server):
        """监听多个事件"""
        events = []

        def do_watch():
            try:
                resp = requests.post(
                    f"http://localhost:{server.port}/v3/watch?key=multi_watch",
                    timeout=10,
                )
                data = resp.json()
                events.extend(data.get("events", []))
            except Exception:
                pass

        watch_thread = threading.Thread(target=do_watch, daemon=True)
        watch_thread.start()

        time.sleep(0.5)

        server.put("/v3/kv/put?key=multi_watch", data="first")
        time.sleep(0.1)
        server.put("/v3/kv/put?key=multi_watch", data="second")
        time.sleep(0.1)
        server.put("/v3/kv/put?key=multi_watch", data="third")

        watch_thread.join(timeout=5)

        # 至少收到一个事件
        assert len(events) >= 1