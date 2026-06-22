import pytest
import json
import time


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
        rev1 = clean_kv.get("rev_key")["mod_revision"]

        clean_kv.put("rev_key", "v2")
        rev2 = clean_kv.get("rev_key")["mod_revision"]

        assert rev2 > rev1


class TestKVRange:
    """测试范围查询"""

    def test_range_prefix(self, clean_kv):
        """前缀范围查询"""
        clean_kv.put("/prefix/a", "1")
        clean_kv.put("/prefix/b", "2")
        clean_kv.put("/prefix/c", "3")
        clean_kv.put("/other/x", "4")

        result = clean_kv.range("/prefix/", "/prefix0")  # /prefix0 > /prefix/
        kvs = result.get("kvs", [])
        assert len(kvs) == 3

    def test_range_all(self, clean_kv):
        """查询所有 key"""
        clean_kv.put("a", "1")
        clean_kv.put("b", "2")
        clean_kv.put("c", "3")

        result = clean_kv.range("\x00", "\xff")
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