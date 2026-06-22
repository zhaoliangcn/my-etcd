import pytest
import json
import requests


class TestClusterInfo:
    """测试集群信息 API"""

    def test_cluster_info(self, server):
        """获取集群信息"""
        resp = server.get("/v3/cluster/info")
        assert resp.status_code == 200

        data = resp.json()
        assert data["node_id"] == 1
        assert data["name"] == "test-node"
        assert data["state"] in ["Leader", "Follower", "Candidate"]
        assert data["term"] >= 0
        assert "revision" in data
        assert "keys" in data

    def test_cluster_info_leader(self, server):
        """单节点应该是 Leader"""
        resp = server.get("/v3/cluster/info")
        data = resp.json()
        assert data["state"] == "Leader"
        assert data["leader_id"] == data["node_id"]

    def test_root_endpoint(self, server):
        """根路径返回 HTML"""
        resp = server.get("/")
        assert resp.status_code == 200
        assert "html" in resp.headers.get("Content-Type", "").lower()
        assert "my-etcd" in resp.text


class TestErrorHandling:
    """测试错误处理"""

    def test_404_not_found(self, server):
        """不存在的路径"""
        resp = server.get("/v3/nonexistent")
        assert resp.status_code == 404

        resp = server.post("/v3/nonexistent")
        assert resp.status_code == 404

    def test_put_missing_key(self, server):
        """PUT 缺少 key 参数"""
        resp = server.put("/v3/kv/put")
        assert resp.status_code == 400

    def test_range_missing_key(self, server):
        """Range 缺少 key 参数"""
        resp = server.post("/v3/kv/range")
        assert resp.status_code == 400

    def test_delete_missing_key(self, server):
        """Delete 缺少 key 参数"""
        resp = server.post("/v3/kv/delete")
        assert resp.status_code == 400

    def test_watch_missing_key(self, server):
        """Watch 缺少 key 参数"""
        resp = server.post("/v3/watch")
        assert resp.status_code == 400


class TestServerHealth:
    """测试服务端健康状态"""

    def test_multiple_requests(self, server):
        """大量请求压测"""
        for i in range(100):
            resp = server.put(f"/v3/kv/put?key=stress_{i}", data=f"data_{i}")
            assert resp.status_code == 200

        for i in range(100):
            resp = server.post(f"/v3/kv/range?key=stress_{i}")
            assert resp.status_code == 200
            kvs = resp.json().get("kvs", [])
            assert len(kvs) == 1
            assert kvs[0]["value"] == f"data_{i}"

    def test_json_content_type(self, server):
        """API 响应 Content-Type 正确"""
        resp = server.put("/v3/kv/put?key=ctype", data="test")
        assert "json" in resp.headers.get("Content-Type", "").lower()

        resp = server.get("/v3/cluster/info")
        assert "json" in resp.headers.get("Content-Type", "").lower()

    def test_response_headers(self, server):
        """响应头包含 Server 标识"""
        resp = server.get("/v3/cluster/info")
        assert "Server" in resp.headers
        assert "my-etcd" in resp.headers["Server"]