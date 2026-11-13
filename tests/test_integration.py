"""端到端集成测试：真实子进程 uvicorn、真实 HTTP Range、断线、进程重启与错误终态。"""
import os
import time

import httpx

from tests.conftest import Server


def make_scenario(run_id, reads=10, **over):
    sc = {
        "run_id": run_id,
        "reads": reads,
        "append_delay": 0.05,
        "r2_extra_delay": 0.1,
        "chunk": 2,
        "interrupt_at_second": 0,
        "interrupt_duration": 0,
        "seq_len": 12,
    }
    sc.update(over)
    return sc


def poll_collect(client, collector, run_id, base_url, timeout=60, expect="completed"):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        r = client.post(
            f"{collector}/collect/runs",
            json={"run_id": run_id, "base_url": base_url},
        )
        last = r.json()
        if last["status"] in ("completed", "error"):
            assert last["status"] == expect, last
            return last, r.status_code
        time.sleep(0.3)
    raise AssertionError(f"采集未在 {timeout}s 内结束: {last}")


def test_happy_path_incremental_pairing_and_download(servers, client):
    relay, collector, _ = servers
    run_id = "happy"
    r = client.post(f"{relay}/runs", json=make_scenario(run_id, reads=10))
    assert r.status_code == 200
    base_url = f"{relay}/runs/{run_id}"

    # 先快速打一轮：数据可能还没写完（running）也可能已写完（completed），都不能报错
    r = client.post(f"{collector}/collect/runs",
                    json={"run_id": run_id, "base_url": base_url})
    assert r.status_code == 200
    assert r.json()["status"] in ("running", "completed")

    snap, _ = poll_collect(client, collector, run_id, base_url)
    assert snap["pairs"] == 10
    assert snap["r1_offset"] > 0 and snap["r2_offset"] > 0

    # 重复调用幂等：不再增加配对
    r = client.post(f"{collector}/collect/runs",
                    json={"run_id": run_id, "base_url": base_url})
    assert r.json()["pairs"] == 10 and r.json()["status"] == "completed"

    r1 = client.get(f"{collector}/collect/runs/{run_id}/R1")
    r2 = client.get(f"{collector}/collect/runs/{run_id}/R2")
    assert r1.status_code == r2.status_code == 200
    l1, l2 = r1.text.splitlines(), r2.text.splitlines()
    assert len(l1) == len(l2) == 40
    assert [l1[i][1:-2] for i in range(0, 40, 4)] == \
           [l2[i][1:-2] for i in range(0, 40, 4)]  # 去后缀后名称一致
    assert all(l1[i].endswith("/1") and l2[i].endswith("/2")
               for i in range(0, 40, 4))


def test_r2_lag_does_not_pair_partial(servers, client):
    relay, collector, _ = servers
    run_id = "lag"
    # R2 每段额外等 0.8s，制造明显的单端滞后窗口
    client.post(f"{relay}/runs",
                json=make_scenario(run_id, reads=6, r2_extra_delay=0.8, chunk=3))
    base_url = f"{relay}/runs/{run_id}"
    deadline = time.time() + 20
    saw_running = False
    while time.time() < deadline:
        r = client.post(f"{collector}/collect/runs",
                        json={"run_id": run_id, "base_url": base_url})
        snap = r.json()
        if snap["status"] == "running":
            saw_running = True
        if snap["status"] == "completed":
            assert saw_running, "应当观察到等待 R2 的运行态"
            assert snap["pairs"] == 6
            return
        if snap["status"] == "error":
            raise AssertionError(f"不应进入错误态: {snap}")
        time.sleep(0.2)
    raise AssertionError("未完成")


def test_interrupted_connection_resumes_without_loss_or_dup(servers, client):
    relay, collector, _ = servers
    run_id = "interrupt"
    client.post(f"{relay}/runs",
                json=make_scenario(run_id, reads=8, append_delay=0.15,
                                   interrupt_at_second=0.5, interrupt_duration=2.5,
                                   chunk=1))
    base_url = f"{relay}/runs/{run_id}"
    notices_seen = []
    deadline = time.time() + 60
    snap = None
    while time.time() < deadline:
        r = client.post(f"{collector}/collect/runs",
                        json={"run_id": run_id, "base_url": base_url})
        snap = r.json()
        notices_seen.extend(snap.get("notices", []))
        if snap["status"] == "completed":
            break
        time.sleep(0.5)
    else:
        raise AssertionError("中断后未能完成采集")
    assert any("中断" in n or "失败" in n for n in notices_seen), "应观察到断线提示"
    assert snap["pairs"] == 8

    # 校验内容与源站最终文件逐字节一致
    src1 = client.get(f"{relay}/runs/{run_id}/R1.fastq").content
    src2 = client.get(f"{relay}/runs/{run_id}/R2.fastq").content
    got1 = client.get(f"{collector}/collect/runs/{run_id}/R1").content
    got2 = client.get(f"{collector}/collect/runs/{run_id}/R2").content
    assert got1 == src1 and got2 == src2


def test_manual_interrupt_window_then_resume(servers, client):
    relay, collector, _ = servers
    run_id = "manual"
    client.post(f"{relay}/runs",
                json=make_scenario(run_id, reads=4, append_delay=0.1))
    base_url = f"{relay}/runs/{run_id}"
    r = client.post(f"{relay}/runs/{run_id}/interrupt", json={"duration": 3})
    assert r.status_code == 200
    snap, _ = poll_collect(client, collector, run_id, base_url)
    assert snap["pairs"] == 4


def test_restart_collector_resumes_from_committed_offsets(servers, client):
    relay, _collector, meta = servers
    run_id = "restart"
    client.post(f"{relay}/runs",
                json=make_scenario(run_id, reads=6, append_delay=0.1, chunk=2))
    base_url = f"{relay}/runs/{run_id}"

    # 独立采集端实例 A 采到至少两对
    srv_a = Server("collector.app:app", {"COLLECTOR_DB": meta["db_path"]})
    srv_a.start()
    try:
        deadline = time.time() + 15
        mid = None
        while time.time() < deadline:
            snap = client.post(f"{srv_a.base_url}/collect/runs",
                               json={"run_id": run_id, "base_url": base_url}).json()
            if snap["pairs"] >= 2:
                mid = snap
                break
            time.sleep(0.1)
        assert mid and mid["pairs"] >= 2
    finally:
        srv_a.stop()  # 进程退出，内存状态全部丢失，只剩 SQLite

    # 全新进程 B 打开同一个 DB 文件继续采
    srv_b = Server("collector.app:app", {"COLLECTOR_DB": meta["db_path"]})
    srv_b.start()
    try:
        # 进度行在重启后立即可见
        r = client.get(f"{srv_b.base_url}/collect/runs/{run_id}")
        assert r.json()["pairs"] == mid["pairs"]

        snap, _ = poll_collect(client, srv_b.base_url, run_id, base_url)
        assert snap["pairs"] == 6
        assert snap["r1_offset"] >= mid["r1_offset"]
        assert snap["r2_offset"] >= mid["r2_offset"]

        # 下载内容与源站逐字节一致（无漏对、无重对）
        src1 = client.get(f"{relay}/runs/{run_id}/R1.fastq").content
        src2 = client.get(f"{relay}/runs/{run_id}/R2.fastq").content
        got1 = client.get(f"{srv_b.base_url}/collect/runs/{run_id}/R1").content
        got2 = client.get(f"{srv_b.base_url}/collect/runs/{run_id}/R2").content
        assert got1 == src1 and got2 == src2
    finally:
        srv_b.stop()


def _write_run(meta, run_id, r1, r2, done=True):
    rdir = os.path.join(meta["relay_data"], run_id)
    os.makedirs(rdir, exist_ok=True)
    with open(os.path.join(rdir, "R1.fastq"), "wb") as f:
        f.write(r1)
    with open(os.path.join(rdir, "R2.fastq"), "wb") as f:
        f.write(r2)
    if done:
        open(os.path.join(rdir, "DONE"), "w").close()
    return rdir


def test_name_mismatch_enters_error_with_both_offsets(servers, client):
    relay, collector, meta = servers
    run_id = "mismatch"
    _write_run(
        meta, run_id,
        b"@alpha/1\nACGT\n+\nIIII\n@beta/1\nACGT\n+\nIIII\n",
        b"@alpha/2\nACGT\n+\nIIII\n@gamma/2\nACGT\n+\nIIII\n",
    )
    r = client.post(f"{collector}/collect/runs",
                    json={"run_id": run_id, "base_url": f"{relay}/runs/{run_id}"})
    assert r.status_code == 422
    snap = r.json()
    assert snap["status"] == "error"
    err = snap["error"]
    assert err["code"] == "name_mismatch"
    assert err["r1_name"] == "beta" and err["r2_name"] == "gamma"
    # 第一条 alpha 已成功配对并提交
    assert snap["pairs"] == 1
    assert "r1_committed_offset" in err and "r2_committed_offset" in err

    # 错误终态下禁止下载
    r = client.get(f"{collector}/collect/runs/{run_id}/R1")
    assert r.status_code == 409


def test_structure_error_reports_positions(servers, client):
    relay, collector, meta = servers
    run_id = "badsuffix"
    _write_run(
        meta, run_id,
        b"@alpha/1\nACGT\n+\nIIII\n@betaX\nACGT\n+\nIIII\n",
        b"@alpha/2\nACGT\n+\nIIII\n@betaX\nACGT\n+\nIIII\n",
    )
    snap = client.post(f"{collector}/collect/runs",
                       json={"run_id": run_id,
                             "base_url": f"{relay}/runs/{run_id}"}).json()
    assert snap["status"] == "error"
    assert snap["error"]["code"] == "fastq_structure_error"
    assert snap["error"]["r1_error_offset"] is not None


def test_orphan_and_residual_after_done(servers, client):
    relay, collector, meta = servers
    # 孤读：R1 多一条
    run_id = "orphan"
    _write_run(
        meta, run_id,
        b"@alpha/1\nACGT\n+\nIIII\n@lonely/1\nACGT\n+\nIIII\n",
        b"@alpha/2\nACGT\n+\nIIII\n",
    )
    snap = client.post(f"{collector}/collect/runs",
                       json={"run_id": run_id,
                             "base_url": f"{relay}/runs/{run_id}"}).json()
    assert snap["status"] == "error" and snap["error"]["code"] == "orphan_read"
    assert snap["error"]["name"] == "lonely"

    # 残条：末条四行没写完
    run_id = "residual"
    _write_run(
        meta, run_id,
        b"@alpha/1\nACGT\n+\nIIII\n@half/1\nAC",
        b"@alpha/2\nACGT\n+\nIIII\n",
    )
    snap = client.post(f"{collector}/collect/runs",
                       json={"run_id": run_id,
                             "base_url": f"{relay}/runs/{run_id}"}).json()
    assert snap["status"] == "error"
    assert snap["error"]["code"] == "residual_records"
    assert snap["error"]["residual_bytes"] > 0


def test_range_support_and_416(servers, client):
    relay, _collector, _meta = servers
    run_id = "rangecheck"
    client.post(f"{relay}/runs", json=make_scenario(run_id, reads=2))
    deadline = time.time() + 10
    while time.time() < deadline:
        try:
            r = client.head(f"{relay}/runs/{run_id}/R1.fastq")
            if r.status_code == 200:
                break
        except httpx.HTTPError:
            pass
        time.sleep(0.1)
    r = client.get(f"{relay}/runs/{run_id}/R1.fastq",
                   headers={"Range": "bytes=0-9"})
    assert r.status_code == 206
    assert r.headers["Content-Range"].startswith("bytes 0-9/")
    assert len(r.content) == 10
    total = int(r.headers["Content-Range"].split("/")[1])
    r = client.get(f"{relay}/runs/{run_id}/R1.fastq",
                   headers={"Range": f"bytes={total}-"})
    assert r.status_code == 416


def test_not_found_and_409_download(servers, client):
    _relay, collector, _meta = servers
    assert client.get(f"{collector}/collect/runs/nope").status_code == 404
    assert client.get(f"{collector}/collect/runs/nope/R1").status_code == 404


def test_error_terminal_state_is_sticky(servers, client):
    relay, collector, meta = servers
    run_id = "sticky"
    _write_run(
        meta, run_id,
        b"@alpha/1\nACGT\n+\nIIII\n@beta/1\nACGT\n+\nIIII\n",
        b"@alpha/2\nACGT\n+\nIIII\n@gamma/2\nACGT\n+\nIIII\n",
    )
    body = {"run_id": run_id, "base_url": f"{relay}/runs/{run_id}"}
    first = client.post(f"{collector}/collect/runs", json=body).json()
    assert first["status"] == "error"
    # 重复调用不会重试、不会改变错误终态与已提交配对
    again = client.post(f"{collector}/collect/runs", json=body).json()
    assert again["status"] == "error"
    assert again["pairs"] == first["pairs"] == 1
    assert again["error"]["code"] == "name_mismatch"


def test_done_visible_but_interrupted_does_not_falsely_finalize(servers, client):
    """DONE 已出现但中断窗口仍在：必须续采到真正 EOF，不能误报残条。"""
    relay, collector, _ = servers
    run_id = "donerace"
    client.post(f"{relay}/runs",
                json=make_scenario(run_id, reads=5, append_delay=0.1,
                                   r2_extra_delay=0.05, chunk=1,
                                   interrupt_at_second=0.3, interrupt_duration=3.0))
    base_url = f"{relay}/runs/{run_id}"
    snap, _ = poll_collect(client, collector, run_id, base_url, timeout=45)
    assert snap["status"] == "completed" and snap["pairs"] == 5
    src = client.get(f"{relay}/runs/{run_id}/R1.fastq").content
    got = client.get(f"{collector}/collect/runs/{run_id}/R1").content
    assert got == src
