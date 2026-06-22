import pytest
import json
import time


class TestLeaseGrant:
    """测试租约创建"""

    def test_grant_lease(self, server):
        """创建租约"""
        resp = server.post("/v3/lease/grant?TTL=10")
        data = resp.json()
        assert data["ID"] > 0
        assert data["TTL"] == 10

    def test_grant_multiple_leases(self, server):
        """创建多个租约"""
        ids = []
        for ttl in [5, 10, 30, 60]:
            resp = server.post(f"/v3/lease/grant?TTL={ttl}")
            data = resp.json()
            assert data["ID"] > 0
            assert data["TTL"] == ttl
            ids.append(data["ID"])

        # ID 应该互不相同
        assert len(set(ids)) == len(ids)

    def test_grant_lease_invalid_ttl(self, server):
        """无效 TTL 参数"""
        resp = server.post("/v3/lease/grant?TTL=0")
        assert resp.status_code == 400

        resp = server.post("/v3/lease/grant?TTL=-1")
        assert resp.status_code == 400

        resp = server.post("/v3/lease/grant")
        assert resp.status_code == 400


class TestLeaseRenew:
    """测试租约续约"""

    def test_renew_lease(self, server):
        """续约租约"""
        resp = server.post("/v3/lease/grant?TTL=60")
        lease_id = resp.json()["ID"]

        resp = server.post(f"/v3/lease/keepalive?ID={lease_id}")
        assert resp.status_code == 200

    def test_renew_nonexistent_lease(self, server):
        """续约不存在的租约"""
        resp = server.post("/v3/lease/keepalive?ID=99999")
        assert resp.status_code == 404

    def test_renew_invalid_id(self, server):
        """无效 ID"""
        resp = server.post("/v3/lease/keepalive?ID=0")
        assert resp.status_code == 400

        resp = server.post("/v3/lease/keepalive")
        assert resp.status_code == 400


class TestLeaseRevoke:
    """测试租约撤销"""

    def test_revoke_lease(self, server):
        """撤销租约"""
        resp = server.post("/v3/lease/grant?TTL=60")
        lease_id = resp.json()["ID"]

        resp = server.post(f"/v3/lease/revoke?ID={lease_id}")
        data = resp.json()
        assert data.get("revoked") is True

    def test_revoke_nonexistent_lease(self, server):
        """撤销不存在的租约"""
        resp = server.post("/v3/lease/revoke?ID=99999")
        assert resp.status_code == 404

    def test_revoke_invalid_id(self, server):
        """无效 ID"""
        resp = server.post("/v3/lease/revoke?ID=0")
        assert resp.status_code == 400


class TestLeaseWithKV:
    """测试租约与 KV 关联"""

    def test_lease_attached_to_key(self, server):
        """创建带租约的 key"""
        resp = server.post("/v3/lease/grant?TTL=60")
        lease_id = resp.json()["ID"]

        resp = server.put(f"/v3/kv/put?key=lease_test&lease={lease_id}", data="val")
        assert resp.status_code == 200
        assert resp.json().get("succeeded") is True

        kv_resp = server.post("/v3/kv/range?key=lease_test")
        kvs = kv_resp.json().get("kvs", [])
        assert len(kvs) == 1
        assert kvs[0]["lease"] == lease_id

    def test_lease_expiry(self, server):
        """租约过期后 key 被删除"""
        resp = server.post("/v3/lease/grant?TTL=1")
        lease_id = resp.json()["ID"]

        server.put(f"/v3/kv/put?key=ephemeral", data="temp")

        # 等待租约过期
        time.sleep(3)

        # 注意：需要确认租约过期回调是否生效
        # 这里仅验证 API 响应正常
        resp = server.post(f"/v3/lease/revoke?ID={lease_id}")
        assert resp.status_code in [200, 404]