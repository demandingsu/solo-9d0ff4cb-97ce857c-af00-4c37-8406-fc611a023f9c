"""端到端演示：启动中转站场景 -> 反复触发增量采集 -> 断线后续采 -> 下载校验。

环境变量：
  RELAY_URL     默认 http://relay:8000
  COLLECTOR_URL 默认 http://collector:8000
  RUN_ID        默认 demo
"""
from __future__ import annotations

import os
import sys
import time

import httpx

RELAY = os.environ.get("RELAY_URL", "http://relay:8000").rstrip("/")
COLLECTOR = os.environ.get("COLLECTOR_URL", "http://collector:8000").rstrip("/")
RUN_ID = os.environ.get("RUN_ID", "demo")


def wait_ready(client: httpx.Client, url: str, timeout: float = 30):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            r = client.get(f"{url}/healthz")
            if r.status_code == 200:
                return
        except httpx.HTTPError:
            pass
        time.sleep(0.5)
    raise RuntimeError(f"服务未就绪: {url}")


def main() -> int:
    with httpx.Client(timeout=30) as c:
        wait_ready(c, RELAY)
        wait_ready(c, COLLECTOR)
        print("[demo] 两个服务均已就绪")

        # 分段追加、R2 滞后 0.4s，启动 1.2s 后中断连接 2s
        scenario = {
            "run_id": RUN_ID,
            "reads": 12,
            "append_delay": 0.2,
            "r2_extra_delay": 0.4,
            "chunk": 2,
            "interrupt_at_second": 1.2,
            "interrupt_duration": 2.0,
            "seq_len": 16,
        }
        r = c.post(f"{RELAY}/runs", json=scenario)
        if r.status_code not in (200, 409):
            raise RuntimeError(f"创建中转站运行失败: {r.status_code} {r.text}")
        print(f"[demo] 中转站场景已创建: {RUN_ID}（含一次连接中断）")

        base_url = f"{RELAY}/runs/{RUN_ID}"
        last_pairs = -1
        deadline = time.time() + 90
        while time.time() < deadline:
            r = c.post(f"{COLLECTOR}/collect/runs",
                       json={"run_id": RUN_ID, "base_url": base_url})
            snap = r.json()
            if snap["pairs"] != last_pairs or snap["status"] != "running":
                print(f"[demo] 采集状态: status={snap['status']} "
                      f"pairs={snap['pairs']} r1={snap['r1_offset']} "
                      f"r2={snap['r2_offset']} notices={snap.get('notices')}")
                last_pairs = snap["pairs"]
            if snap["status"] == "error":
                print(f"[demo] 采集失败: {snap['error']}")
                return 1
            if snap["status"] == "completed":
                break
            time.sleep(0.8)
        else:
            print("[demo] 等待完成超时")
            return 1

        # 下载并校验
        for side, expect_suffix in ((1, "/1"), (2, "/2")):
            r = c.get(f"{COLLECTOR}/collect/runs/{RUN_ID}/R{side}")
            r.raise_for_status()
            lines = r.content.decode().splitlines()
            assert len(lines) == scenario["reads"] * 4, "下载行数不符"
            assert all(lines[i].startswith("@") and lines[i].endswith(expect_suffix)
                       for i in range(0, len(lines), 4)), "名称后缀不符"
            assert all(len(lines[i + 1]) == len(lines[i + 3])
                       for i in range(0, len(lines), 4)), "序质长度不等"
            print(f"[demo] R{side} 下载校验通过: {scenario['reads']} 条，"
                  f"{len(r.content)} 字节")

        # 进度总览
        r = c.get(f"{COLLECTOR}/collect/runs/{RUN_ID}")
        print(f"[demo] 最终进度: {r.json()}")
        print("[demo] 全部完成 ✔")
        return 0


if __name__ == "__main__":
    sys.exit(main())
