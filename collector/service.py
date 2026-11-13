"""增量采集服务：HTTP Range 续采 + 内存配对状态机 + SQLite 原子提交。

持久化边界只有一个：每成功配对一条记录，就在单个 SQLite 事务中同时
写入配对内容与两端新偏移。进程重启后，已提交的配对与偏移不会重复，
而尚未配对的残条/孤读会留在源文件中，由下一次采集按旧偏移重新拉取。

错误分类：
* 瞬时问题（连接失败、流中断、源站暂时 5xx）——不改变 running 状态，
  已收字节保留在内存，下一次调用按 Range 断点续采；
* 数据问题（结构错误、名称不匹配、完成后残条/孤读）——进入 error 终态，
  停止采集并回报两端字节位置。
"""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field

import httpx

from . import db
from .parser import FastqFormatError, Record, split_fastq, validate

DONE_NAME = "DONE"
R1_NAME = "R1.fastq"
R2_NAME = "R2.fastq"
IDLE_DEADLINE = 4  # 一轮采集中两端连续没有新字节的轮次上限


class CollectorError(RuntimeError):
    """采集进入不可恢复的数据错误终态。"""

    def __init__(self, code: str, message: str, detail: dict):
        super().__init__(message)
        self.code = code
        self.detail = detail


@dataclass
class SideState:
    offset: int = 0                 # 已提交（已配对）的字节进度
    buffer: bytearray = field(default_factory=bytearray)  # 始终从 offset 开始
    parsed: int = 0                 # buffer 中已解析成完整记录的字节数
    queue: list[Record] = field(default_factory=list)

    def parse(self, side: int) -> None:
        """解析新增字节并把完整记录入队。

        缓冲与 parsed 不在此处裁剪：已解析记录可能仍在等待另一端配对，
        统一由 :meth:`advance` 在配对提交后按新偏移裁剪。
        """
        records, _leftover, _parsed, _, _ = split_fastq(
            bytes(self.buffer), self.parsed, self.offset
        )
        for rec in records:
            validate(rec, side)
            self.queue.append(rec)
        self.parsed = len(self.buffer)  # 全部字节都已尝试过解析，残条等下次新数据

    def advance(self, new_offset: int) -> None:
        """提交后前推偏移，并丢弃已配对字节。"""
        drop = new_offset - self.offset
        if drop < 0 or drop > len(self.buffer):
            raise AssertionError(f"非法的缓冲裁剪: drop={drop}")
        del self.buffer[:drop]
        self.offset = new_offset
        self.parsed = max(0, self.parsed - drop)

    @property
    def residual_tail(self) -> int:
        """缓冲中不属于任何完整记录的残条尾字节数。"""
        queued = self.queue[-1].end - self.offset if self.queue else 0
        return len(self.buffer) - queued


class CollectorService:
    def __init__(self, conn, timeout: float = 10.0):
        self.conn = conn
        self.timeout = timeout
        self._client: httpx.AsyncClient | None = None
        self._locks: dict[str, asyncio.Lock] = {}
        self._sides: dict[str, dict[int, SideState]] = {}

    async def aclose(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    def _lock(self, run_id: str) -> asyncio.Lock:
        lock = self._locks.get(run_id)
        if lock is None:
            lock = asyncio.Lock()
            self._locks[run_id] = lock
        return lock

    def _live(self, run_id: str) -> dict[int, SideState]:
        sides = self._sides.get(run_id)
        if sides is None:
            row = db.get_run(self.conn, run_id)
            sides = {
                1: SideState(offset=row["r1_offset"]),
                2: SideState(offset=row["r2_offset"]),
            }
            self._sides[run_id] = sides
        return sides

    async def _http(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=self.timeout, follow_redirects=True)
        return self._client

    # ------------------------------------------------------------------ 主流程
    async def collect(self, run_id: str, base_url: str) -> dict:
        async with self._lock(run_id):
            now = time.time()
            db.create_run(self.conn, run_id, base_url, now)
            row = db.get_run(self.conn, run_id)
            if row["status"] in ("completed", "error"):
                return db.snapshot(row)

            sides = self._live(run_id)
            notices: list[str] = []
            idle_rounds = 0
            try:
                while True:
                    advanced = False
                    for side in (1, 2):
                        got, problem, _eof = await self._fetch_side(
                            base_url, sides, side
                        )
                        if got:
                            advanced = True
                        if problem:
                            notices.append(problem)
                    await self._pair(run_id, sides)

                    done = await self._source_done(base_url)
                    if done:
                        # 源端已完成：必须两端都确认到明确 EOF 才做终态校验，
                        # 断线/未拉全时继续下一轮按 Range 续采
                        eof_confirmed = True
                        for side in (1, 2):
                            got, problem, eof = await self._fetch_side(
                                base_url, sides, side
                            )
                            if problem:
                                notices.append(problem)
                            if got:
                                advanced = True
                            if not eof:
                                eof_confirmed = False
                        await self._pair(run_id, sides)
                        if eof_confirmed:
                            self._finalize(run_id, sides)
                            return db.snapshot(db.get_run(self.conn, run_id))
                    if not advanced:
                        idle_rounds += 1
                        if idle_rounds >= IDLE_DEADLINE:
                            notices.append("源端暂无可采集的新字节，等待后续追加")
                            break
                        await asyncio.sleep(0.2)
                    else:
                        idle_rounds = 0
            except CollectorError as exc:
                db.mark_status(
                    self.conn, run_id, "error", time.time(),
                    {"code": exc.code, "message": str(exc), **exc.detail},
                )
            snap = db.snapshot(db.get_run(self.conn, run_id))
            if notices:
                # 去重后透传瞬时情况，状态仍为 running
                snap["notices"] = list(dict.fromkeys(notices))
            return snap

    # ------------------------------------------------------------- 单端 Range 拉取
    async def _fetch_side(
        self, base_url: str, sides: dict[int, SideState], side: int
    ) -> tuple[bool, str | None, bool]:
        """从已提交偏移+缓冲长度处续采。

        返回 (本轮是否收到新字节, 瞬时问题描述, 是否确认到 EOF)。
        EOF 仅在源站明确返回 416/404 且无传输错误时成立；传输中断
        不抛终态错误，已收到的字节保留，下次按 Range 继续。
        """
        st = sides[side]
        client = await self._http()
        url = f"{base_url.rstrip('/')}/{R1_NAME if side == 1 else R2_NAME}"
        start = st.offset + len(st.buffer)
        try:
            req = client.build_request(
                "GET", url, headers={"Range": f"bytes={start}-"}
            )
            resp = await client.send(req, stream=True)
        except httpx.HTTPError as exc:
            return False, f"R{side} 连接源站失败（保留断点 {start}）: {exc}", False

        problem: str | None = None
        eof = False
        try:
            if resp.status_code in (404, 416):
                return False, None, True  # 文件尚未创建 / 已到 EOF
            if resp.status_code >= 500:
                # 源站瞬时故障/中断：保留断点，下轮按 Range 重试
                word = "中断" if resp.status_code == 503 else "故障"
                return False, (
                    f"R{side} 源站连接{word}（HTTP {resp.status_code}），"
                    f"保留断点 {start}，等待重试"
                ), False
            if resp.status_code != 206 and not (resp.status_code == 200 and start == 0):
                raise CollectorError(
                    "source_http_error",
                    f"源站返回不可恢复的 HTTP {resp.status_code}",
                    {"url": url, "r1_offset": sides[1].offset,
                     "r2_offset": sides[2].offset},
                )
            if resp.status_code == 200 and start != 0:
                # 源站不支持 Range：继续会破坏"已有字节不改写"的前提
                raise CollectorError(
                    "range_unsupported",
                    "源站未按 Range 请求返回 206，无法保证增量字节一致性",
                    {"url": url, "requested_offset": start,
                     "r1_offset": sides[1].offset, "r2_offset": sides[2].offset},
                )
            got = False
            try:
                async for chunk in resp.aiter_bytes(64 * 1024):
                    if chunk:
                        st.buffer.extend(chunk)
                        got = True
            except httpx.HTTPError as exc:
                problem = (
                    f"R{side} 传输中断，已保留 {st.offset + len(st.buffer)} 字节"
                    f"断点，下次续采: {exc}"
                )
            if not problem:
                # 完整读完本次 Range 响应：若响应短于请求区间即到当前 EOF
                content_range = resp.headers.get("content-range", "")
                if content_range:
                    total = int(content_range.rsplit("/", 1)[1])
                    eof = st.offset + len(st.buffer) >= total
                elif resp.status_code == 200:
                    eof = True
            return got, problem, eof
        finally:
            await resp.aclose()

    # ------------------------------------------------------------------ 配对状态机
    async def _pair(self, run_id: str, sides: dict[int, SideState]) -> None:
        for side in (1, 2):
            st = sides[side]
            try:
                st.parse(side)
            except FastqFormatError as exc:
                self._fail_structure(sides, side, exc)

        row = db.get_run(self.conn, run_id)
        seq = row["pairs"]
        q1, q2 = sides[1].queue, sides[2].queue
        cut = 0
        last_ends: dict[int, int] = {}
        while cut < len(q1) and cut < len(q2):
            r1, r2 = q1[cut], q2[cut]
            if r1.name != r2.name:
                self._fail_name(sides, r1, r2, seq + cut)
            db.commit_pair(
                self.conn, run_id, seq + cut, r1.name, r1.raw, r2.raw,
                r1.end, r2.end, time.time(),
            )
            last_ends[1], last_ends[2] = r1.end, r2.end
            cut += 1
        if cut:
            del q1[:cut]
            del q2[:cut]
            # 单事务已提交，前推两端内存偏移并丢弃已配对字节
            for side, end in last_ends.items():
                sides[side].advance(end)

    def _fail_structure(
        self, sides: dict[int, SideState], side: int, exc: FastqFormatError
    ):
        other = 2 if side == 1 else 1
        raise CollectorError(
            "fastq_structure_error", f"R{side} FASTQ 结构错误",
            {
                "reason": str(exc),
                "side": side,
                f"r{side}_committed_offset": sides[side].offset,
                f"r{other}_committed_offset": sides[other].offset,
                f"r{side}_error_offset": exc.offset,
            },
        )

    def _fail_name(self, sides: dict[int, SideState], r1: Record, r2: Record, seq: int):
        raise CollectorError(
            "name_mismatch",
            f"读段名不匹配: R1={r1.name!r} 与 R2={r2.name!r}",
            {
                "pair_index": seq,
                "r1_name": r1.name,
                "r2_name": r2.name,
                "r1_record_start": r1.start,
                "r2_record_start": r2.start,
                "r1_committed_offset": sides[1].offset,
                "r2_committed_offset": sides[2].offset,
            },
        )

    # ------------------------------------------------------------------ 终态校验
    def _finalize(self, run_id: str, sides: dict[int, SideState]) -> None:
        # 1) 残条：不属于任何完整记录的未写完字节
        for side in (1, 2):
            st = sides[side]
            other = 2 if side == 1 else 1
            residual = st.residual_tail
            if residual:
                raise CollectorError(
                    "residual_records",
                    f"源端完成后 R{side} 仍有未写完的残缺记录",
                    {
                        "side": side,
                        f"r{side}_committed_offset": st.offset,
                        f"r{other}_committed_offset": sides[other].offset,
                        "residual_bytes": residual,
                    },
                )
        for side in (1, 2):
            st = sides[side]
            other = 2 if side == 1 else 1
            if st.queue:
                rec = st.queue[0]
                raise CollectorError(
                    "orphan_read",
                    f"源端完成后 R{side} 存在没有另一端配对的孤读: {rec.name!r}",
                    {
                        "side": side,
                        "name": rec.name,
                        "record_start": rec.start,
                        f"r{side}_committed_offset": st.offset,
                        f"r{other}_committed_offset": sides[other].offset,
                    },
                )
        db.mark_status(self.conn, run_id, "completed", time.time())

    async def _source_done(self, base_url: str) -> bool:
        client = await self._http()
        url = f"{base_url.rstrip('/')}/{DONE_NAME}"
        try:
            resp = await client.head(url)
        except httpx.HTTPError:
            return False
        try:
            return resp.status_code == 200
        finally:
            await resp.aclose()
