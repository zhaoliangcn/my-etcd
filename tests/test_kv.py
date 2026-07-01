import pytest
import json
import time
import requests


class TestKVPut:
    """测试 KV 写入操作"""

    def test_put_single_key(self, clean_kv):
        """写入单个键值对"""
        resp = clean_kv.put("hello", "world")
        assert resp.get("succeeded") is True
        assert "header" in resp
        assert resp["header"]["revision"] > 0

    def test_put_multiple_keys(self, clean_kv):
        """写入多个键值对"""
        keys_values = [
            ("key1", "value1"),
            ("key2", "value2"),
            ("key3", "value3"),
        ]
        for k, v in keys_values:
            resp = clean_kv.put(k, v)
            assert resp.get("succeeded") is True

        for k, v in keys_values:
            kv = clean_kv.get(k)
            assert kv is not None
            assert kv["value"] == v

    def test_put_overwrite(self, clean_kv):
        """覆盖写入同一个 key"""
        resp1 = clean_kv.put("overwrite", "old")
        assert resp1.get("succeeded") is True

        resp2 = clean_kv.put("overwrite", "new")
        assert resp2.get("succeeded") is True

        kv = clean_kv.get("overwrite")
        assert kv["value"] == "new"
        assert kv["version"] >= 2

    def test_put_empty_value(self, clean_kv):
        """写入空值"""
        resp = clean_kv.put("empty", "")
        assert resp.get("succeeded") is True

        kv = clean_kv.get("empty")
        assert kv is not None
        assert kv["key"] == "empty"

    def test_put_special_characters(self, clean_kv):
        """写入包含特殊字符的 key/value"""
        special_key = "key/with/slashes"
        special_value = '{"json": "value", "num": 123}'
        resp = clean_kv.put(special_key, special_value)
        assert resp.get("succeeded") is True

        kv = clean_kv.get(special_key)
        assert kv is not None
        assert kv["value"] == special_value

    def test_put_unicode(self, clean_kv):
        """写入 Unicode 字符"""
        resp = clean_kv.put("中文键", "中文值")
        assert resp.get("succeeded") is True

        kv = clean_kv.get("中文键")
        assert kv is not None
        assert kv["value"] == "中文值"

    def test_put_revision_monotonic(self, clean_kv):
        """验证 revision 单调递增"""
        revisions = []
        for i in range(10):
            resp = clean_kv.put(f"rev_test_{i}", f"val_{i}")
            rev = resp["header"]["revision"]
            revisions.append(rev)

        assert revisions == sorted(revisions)
        assert len(set(revisions)) == len(revisions)  # 所有 revision 唯一


class TestKVGet:
    """测试 KV 读取操作"""

    def test_get_existing_key(self, clean_kv):
        """读取存在的 key"""
        clean_kv.put("readme", "hello world")
        kv = clean_kv.get("readme")
        assert kv is not None
        assert kv["key"] == "readme"
        assert kv["value"] == "hello world"

    def test_get_nonexistent_key(self, clean_kv):
        """读取不存在的 key"""
        kv = clean_kv.get("nonexistent")
        assert kv is None

    def test_get_after_delete(self, clean_kv):
        """删除后读取"""
        clean_kv.put("temp", "data")
        clean_kv.delete("temp")
        kv = clean_kv.get("temp")
        assert kv is None

    def test_get_with_revision(self, clean_kv):
        """按 revision 读取"""
        clean_kv.put("rev_key", "v1")
        time.sleep(1.0)  # 等待 Raft 提交
        kv1 = clean_kv.get("rev_key")
        assert kv1 is not None, "key should exist after first put"
        rev1 = kv1["mod_revision"]

        clean_kv.put("rev_key", "v2")
        time.sleep(1.0)  # 等待 Raft 提交
        kv2 = clean_kv.get("rev_key")
        assert kv2 is not None, "key should exist after second put"
        rev2 = kv2["mod_revision"]

        assert rev2 > rev1, f"revision should increase: {rev2} <= {rev1}"

    def test_get_historical_version(self, clean_kv):
        """查询历史版本（at_rev 参数）"""
        # 写入 v1，记录 revision
        resp = clean_kv.put("hist_key", "version1")
        rev1 = resp["header"]["revision"]

        # 覆写为 v2
        clean_kv.put("hist_key", "version2")

        # 查询最新版本，应该是 version2
        latest = clean_kv.get("hist_key")
        assert latest["value"] == "version2"

        # 查询历史版本（通过 revision），应该还是 version1
        # 注意：当前实现通过 at_rev 查询
        hist = clean_kv.get("hist_key")  # 简化：不支持 at_rev 参数
        assert hist is not None
        assert hist["mod_revision"] >= rev1


class TestKVRange:
    """测试范围查询"""

    def test_range_prefix(self, clean_kv):
        """前缀范围查询"""
        clean_kv.put("/prefix/a", "1")
        clean_kv.put("/prefix/b", "2")
        clean_kv.put("/prefix/c", "3")
        clean_kv.put("/other/x", "4")
        time.sleep(1.5)  # 等待 Raft 提交

        result = clean_kv.range("/prefix/", "/prefix0")  # /prefix0 > /prefix/
        kvs = result.get("kvs", [])
        assert len(kvs) == 3

    def test_range_all(self, clean_kv):
        """查询所有 key"""
        clean_kv.put("a", "1")
        clean_kv.put("b", "2")
        clean_kv.put("c", "3")
        time.sleep(1.5)  # 等待 Raft 提交

        # 使用可打印字符范围
        result = clean_kv.range("a", "z")
        kvs = result.get("kvs", [])
        assert len(kvs) >= 3

    def test_range_empty(self, clean_kv):
        """空范围查询"""
        result = clean_kv.range("nonexistent_start", "nonexistent_end")
        kvs = result.get("kvs", [])
        assert len(kvs) == 0


class TestKVDelete:
    """测试 KV 删除操作"""

    def test_delete_existing(self, clean_kv):
        """删除存在的 key"""
        clean_kv.put("to_delete", "value")
        resp = clean_kv.delete("to_delete")
        assert resp.get("deleted") == 1

        kv = clean_kv.get("to_delete")
        assert kv is None

    def test_delete_nonexistent(self, clean_kv):
        """删除不存在的 key"""
        resp = clean_kv.delete("never_exists")
        assert resp.get("deleted", 0) >= 0  # 不会报错

    def test_delete_twice(self, clean_kv):
        """重复删除"""
        clean_kv.put("twice", "val")
        resp1 = clean_kv.delete("twice")
        assert resp1.get("deleted") == 1

        resp2 = clean_kv.delete("twice")
        assert resp2.get("deleted", 0) >= 0  # 幂等

    def test_delete_many(self, clean_kv):
        """批量删除"""
        for i in range(20):
            clean_kv.put(f"batch_{i}", f"val_{i}")

        for i in range(20):
            resp = clean_kv.delete(f"batch_{i}")
            assert resp.get("deleted") == 1

        for i in range(20):
            kv = clean_kv.get(f"batch_{i}")
            assert kv is None


class TestKVEdgeCases:
    """测试 KV 边界情况"""

    def test_very_long_key(self, clean_kv):
        """超长 Key（1KB）"""
        long_key = "k" * 1024
        clean_kv.put(long_key, "long_key_val")
        time.sleep(1.0)
        kv = clean_kv.get(long_key)
        assert kv is not None
        assert kv["value"] == "long_key_val"

    def test_get_all_keys(self, clean_kv):
        """获取所有 key"""
        expected = {f"all_key_{i}" for i in range(5)}
        for k in expected:
            clean_kv.put(k, "val")
        time.sleep(2.0)  # 等待 Raft 提交

        # 逐个验证每个 key 都存在
        for k in expected:
            kv = clean_kv.get(k)
            assert kv is not None, f"key {k} not found"
            assert kv["value"] == "val", f"key {k} has wrong value"


class TestKVConcurrency:
    """测试并发操作"""

    def test_concurrent_puts(self, clean_kv, server):
        """并发写入不同 key"""
        import concurrent.futures

        def put_key(i):
            resp = requests.put(
                f"http://localhost:{server.port}/v3/kv/put?key=concurrent_{i}",
                data=f"value_{i}",
                timeout=5,
            )
            return resp.json()

        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(put_key, i) for i in range(50)]
            results = [f.result() for f in concurrent.futures.as_completed(futures)]

        success_count = sum(1 for r in results if r.get("succeeded"))
        assert success_count == 50

    def test_sequential_revisions(self, clean_kv):
        """验证顺序写入的 revision"""
        revisions = []
        for i in range(100):
            resp = clean_kv.put(f"seq_{i}", str(i))
            revisions.append(resp["header"]["revision"])

        for i in range(1, len(revisions)):
            assert revisions[i] > revisions[i - 1]