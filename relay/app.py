"""模拟测序仪器中转站。

能力：
1. 提供运行目录的 HTTP 文件服务，支持 Range（206）与 HEAD；
2. 按场景脚本在后台分段追加 R1.fastq / R2.fastq，可配置每段延迟、
   块大小，以及单端（R2）额外的滞后延迟；
3. 写完全部读段后放置 DONE 完成标志；
4. 支持"中断连接"：中断窗口内对 FASTQ 的 GET 请求在传输若干字节后
   主动断开（连接重置），用于验证采集端断线续采。
"""
from __future__ import annotations

import asyncio
import json
import os
import time
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse
from pydantic import BaseModel, Field

DATA_ROOT = Path(os.environ.get("RELAY_DATA", "/data/relay")).resolve()
DATA_ROOT.mkdir(parents=True, exist_ok=True)

# run_id -> 运行态字典
_runs: dict[str, dict] = {}


class Scenario(BaseModel):
    run_id: str = Field(..., min_length=1, max_length=128, pattern=r"^[A-Za-z0-9_.-]+$")
    reads: int = Field(20, ge=1, le=100_000)
    append_delay: float = Field(0.15, ge=0, description="每段追加之间的秒数")
    r2_extra_delay: float = Field(0.3, ge=0, description="R2 每段额外滞后秒数")
    chunk: int = Field(2, ge=1, description="每次追加的读段条数")
    interrupt_at_second: float = Field(
        0.0, ge=0, description=">0 时，启动后该秒自动进入中断窗口"
    )
    interrupt_duration: float = Field(2.0, ge=0, description="中断窗口持续秒数")
    seq_len: int = Field(12, ge=1, le=200)


def _run_dir(run_id: str) -> Path:
    p = (DATA_ROOT / run_id).resolve()
    if DATA_ROOT not in p.parents and p != DATA_ROOT:
        raise HTTPException(status_code=400, detail="非法 run_id")
    return p


def _record(seq_no: int, side: int, seq_len: int) -> bytes:
    # 确定性的伪序列/质量串，避免引入随机依赖
    name = f"seq{seq_no:05d}/{side}"
    seq = "".join("ACGT"[(seq_no + i + side) % 4] for i in range(seq_len))
    qual = "".join(chr(33 + ((seq_no * 7 + i * 3) % 60)) for i in range(seq_len))
    return f"@{name}\n{seq}\n+\n{qual}\n".encode("ascii")


def _flush(fh, blob: bytes, path: Path) -> int:
    """在线程池中执行的阻塞写盘。"""
    fh.write(blob)
    fh.flush()
    os.fsync(fh.fileno())
    return path.stat().st_size


async def _writer(state: dict, sc: Scenario):
    rdir = await asyncio.to_thread(_run_dir, sc.run_id)
    await asyncio.to_thread(rdir.mkdir, 0o777, True, True)
    r1 = open(rdir / "R1.fastq", "wb")
    r2 = open(rdir / "R2.fastq", "wb")
    p1, p2 = rdir / "R1.fastq", rdir / "R2.fastq"
    state["started_at"] = time.time()
    try:
        if sc.interrupt_at_second > 0:
            asyncio.create_task(_auto_interrupt(sc.run_id, sc))
        written = 0
        while written < sc.reads:
            nxt = min(written + sc.chunk, sc.reads)
            blob1 = b"".join(_record(i, 1, sc.seq_len) for i in range(written, nxt))
            state["r1_bytes"] = await asyncio.to_thread(_flush, r1, blob1, p1)
            state["r1_reads"] = nxt
            # 单端滞后：R2 的写入慢半拍
            await asyncio.sleep(sc.r2_extra_delay)
            blob2 = b"".join(_record(i, 2, sc.seq_len) for i in range(written, nxt))
            state["r2_bytes"] = await asyncio.to_thread(_flush, r2, blob2, p2)
            state["r2_reads"] = nxt
            written = nxt
            if written < sc.reads:
                await asyncio.sleep(sc.append_delay)
        # 全部落盘后再放完成标志
        def _write_done():
            (rdir / "DONE").write_text(
                json.dumps({"reads": sc.reads, "finished_at": time.time()}) + "\n"
            )
        await asyncio.to_thread(_write_done)
        state["done"] = True
        state["finished_at"] = time.time()
    except Exception as exc:  # 写失败也要让状态可见
        state["writer_error"] = repr(exc)
    finally:
        r1.close()
        r2.close()


async def _auto_interrupt(run_id: str, sc: Scenario):
    await asyncio.sleep(sc.interrupt_at_second)
    until = time.time() + sc.interrupt_duration
    _runs[run_id]["interrupt_until"] = until
    await asyncio.sleep(sc.interrupt_duration)
    st = _runs.get(run_id)
    # 仅当窗口没有被其它请求重新打开时才自动恢复
    if st and st.get("interrupt_until", 0) == until:
        st["interrupt_until"] = 0.0


app = FastAPI(title="仪器中转站（模拟）", version="1.0.0")


@app.exception_handler(ConnectionResetError)
async def _reset_handler(request: Request, exc: ConnectionResetError):
    # 中断窗口的连接重置：返回干净的 503，由采集端按 Range 断点重试，
    # 不向 stderr 打印 traceback（高频中断下会撑满日志管道）
    return JSONResponse(
        status_code=503,
        content={"detail": "仪器中转站连接中断，请按字节断点重试"},
        headers={"Retry-After": "1"},
    )


@app.get("/healthz")
def healthz():
    return {"status": "ok"}


@app.post("/runs")
async def create_run(sc: Scenario):
    if sc.run_id in _runs:
        raise HTTPException(status_code=409, detail=f"运行 {sc.run_id} 已存在")
    rdir = _run_dir(sc.run_id)
    if rdir.exists() and any(rdir.iterdir()):
        raise HTTPException(status_code=409, detail="运行目录已存在且非空")
    rdir.mkdir(parents=True, exist_ok=True)
    state = {
        "scenario": sc.model_dump(),
        "r1_reads": 0, "r2_reads": 0,
        "r1_bytes": 0, "r2_bytes": 0,
        "done": False, "started_at": time.time(),
        "interrupt_until": 0.0,
        "writer_error": None,
    }
    _runs[sc.run_id] = state
    asyncio.create_task(_writer(state, sc))
    return {"run_id": sc.run_id, "base_url": f"/runs/{sc.run_id}"}


@app.get("/runs")
def list_runs():
    return {"runs": list(_runs.keys())}


@app.get("/runs/{run_id}/status")
def run_status(run_id: str):
    st = _runs.get(run_id)
    if st is None:
        raise HTTPException(status_code=404, detail="运行不存在")
    now = time.time()
    return {
        **{k: v for k, v in st.items() if k != "scenario"},
        "interrupting": now < st.get("interrupt_until", 0),
        "scenario": st["scenario"],
    }


class InterruptReq(BaseModel):
    duration: float = Field(2.0, ge=0, le=120)


@app.post("/runs/{run_id}/interrupt")
def interrupt(run_id: str, req: InterruptReq):
    st = _runs.get(run_id)
    if st is None:
        raise HTTPException(status_code=404, detail="运行不存在")
    st["interrupt_until"] = time.time() + req.duration
    return {"interrupt_until": st["interrupt_until"], "duration": req.duration}


@app.post("/runs/{run_id}/resume")
def resume(run_id: str):
    st = _runs.get(run_id)
    if st is None:
        raise HTTPException(status_code=404, detail="运行不存在")
    st["interrupt_until"] = 0.0
    return {"resumed": True}


def _resolve(run_id: str, name: str) -> Path:
    if name not in ("R1.fastq", "R2.fastq", "DONE"):
        raise HTTPException(status_code=404, detail="仅允许访问 R1.fastq/R2.fastq/DONE")
    path = _run_dir(run_id) / name
    if not path.is_file():
        raise HTTPException(status_code=404, detail=f"{name} 尚不存在")
    return path


def _interrupting(run_id: str) -> bool:
    st = _runs.get(run_id)
    return bool(st and time.time() < st.get("interrupt_until", 0))


@app.api_route("/runs/{run_id}/{name}", methods=["GET", "HEAD"])
async def serve_file(run_id: str, name: str, request: Request):
    path = _resolve(run_id, name)
    size = path.stat().st_size
    headers = {"Accept-Ranges": "bytes", "Content-Type": "text/plain; charset=utf-8"}

    # 中断窗口：对 FASTQ 的 GET 在响应开始前直接重置连接
    if request.method == "GET" and name.endswith(".fastq") and _interrupting(run_id):
        # 让传输层关闭连接，模拟仪器侧连接重置（客户端收到 503 后按断点重试）
        raise ConnectionResetError("模拟仪器中转站连接中断")

    range_header = request.headers.get("range")
    start, end = 0, size - 1
    status = 200
    if range_header:
        if not range_header.startswith("bytes="):
            raise HTTPException(status_code=400, detail="仅支持 bytes Range")
        spec = range_header[6:]
        if "," in spec:
            raise HTTPException(status_code=400, detail="不支持多区间 Range")
        a, _, b = spec.partition("-")
        try:
            if a == "":  # 后缀形式 bytes=-N
                length = int(b)
                start, end = max(0, size - length), size - 1
            else:
                start = int(a)
                end = int(b) if b else size - 1
        except ValueError:
            raise HTTPException(status_code=400, detail="Range 格式非法")
        if start >= size or start < 0 or end < start:
            headers["Content-Range"] = f"bytes */{size}"
            return Response(status_code=416, headers=headers)
        end = min(end, size - 1)
        status = 206
        headers["Content-Range"] = f"bytes {start}-{end}/{size}"

    length = end - start + 1
    headers["Content-Length"] = str(length)
    if request.method == "HEAD":
        return Response(status_code=status, headers=headers)

    async def stream():
        with open(path, "rb") as fh:
            fh.seek(start)
            remaining = length
            while remaining > 0:
                # 在线程池中读盘，避免阻塞事件循环（写线程也在同一进程）
                chunk = await asyncio.to_thread(fh.read, min(64 * 1024, remaining))
                if not chunk:
                    return
                yield chunk
                remaining -= len(chunk)

    return StreamingResponse(stream(), status_code=status, headers=headers)
