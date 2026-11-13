"""FASTQ 解析与配对名称处理。

约定：仅处理未压缩 FASTQ；每条记录严格四行：
    @name/suffix\n
    sequence\n
    +\n
    quality\n
其中 R1 名称以 /1 结尾、R2 以 /2 结尾，去掉该后缀后应完全一致。

解析在字节层面进行，使"已消费字节数"与源文件实际字节偏移严格对应，
断点续采时按该偏移发起 HTTP Range 请求即可。
"""
from __future__ import annotations

from dataclasses import dataclass


class FastqFormatError(ValueError):
    """FASTQ 结构或内容错误（缺行、头部非法、序质长度不等……）。"""

    def __init__(self, message: str, offset: int | None = None):
        super().__init__(message)
        self.offset = offset


@dataclass
class Record:
    """一条完整的四行 FASTQ 记录（原始字节）及其在源文件中的起止字节偏移。"""

    name: str          # 去掉 /1 或 /2 后缀后的读段名
    raw: bytes         # 含四行及各自换行
    start: int         # 记录首字节（'@'）在源文件中的绝对偏移
    end: int           # 记录结束位置（下一条记录的起点，EOF 时等于文件长度）

    def suffix_ok(self, expect: int) -> bool:
        """检查名称后缀是否为期望的 /1 或 /2。"""
        head = self.raw.split(b"\n", 1)[0]
        return head.endswith((f"/{expect}").encode())


def split_fastq(
    buffer: bytes, parsed: int, base: int
) -> tuple[list[Record], bytes, int, bytes | None, int]:
    """从已累积的缓冲区中切出所有完整的四行记录。

    参数:
        buffer: 自 ``base`` 偏移起、尚未完整解析的全部字节
        parsed: buffer 中已被前次解析消费的字节数
        base:   buffer 首字节对应的源文件绝对偏移

    返回:
        records:           完整记录（start/end 为绝对字节偏移）
        new_buffer:        未消费部分（残缺记录）
        new_parsed:        新缓冲区中已解析消费的字节数（通常为 0）
        partial_bytes:     若缓冲区以一条没有结尾换行的不完整行结束，则返回
                           该行原始字节；否则为 None
        partial_offset:    partial_bytes 首字节的绝对偏移
    """
    records: list[Record] = []
    lines: list[tuple[int, int]] = []   # (相对 buffer 的起点, 行长度含换行)
    pos = parsed
    n = len(buffer)
    partial_bytes: bytes | None = None
    partial_offset = base + pos

    while pos < n:
        nl = buffer.find(b"\n", pos)
        if nl == -1:
            # 末行未写完：保留等待后续数据
            partial_bytes = buffer[pos:]
            partial_offset = base + pos
            break
        lines.append((pos, nl + 1 - pos))
        pos = nl + 1

    nblocks = len(lines) // 4
    for i in range(nblocks):
        block = lines[i * 4:(i + 1) * 4]
        start = block[0][0]
        end = block[3][0] + block[3][1]
        raw = buffer[start:end]
        head = raw[: raw.find(b"\n")]
        if not head.startswith(b"@") or len(head) < 2:
            raise FastqFormatError(
                f"FASTQ 记录头部非法（应以 '@名称' 开头）: {head[:40]!r}",
                offset=base + start,
            )
        try:
            name = head[1:].decode("ascii")
        except UnicodeDecodeError:
            raise FastqFormatError("FASTQ 名称包含非 ASCII 字节", offset=base + start)
        if not (name.endswith("/1") or name.endswith("/2")):
            raise FastqFormatError(
                f"读段名未以 /1 或 /2 结尾: {name!r}", offset=base + start
            )
        records.append(
            Record(name=name[:-2], raw=raw, start=base + start, end=base + end)
        )

    consumed = 0
    if nblocks:
        consumed = lines[nblocks * 4 - 1][0] + lines[nblocks * 4 - 1][1]

    leftover = buffer[consumed:]
    # partial 位于所有完整行之后，必然留在 leftover 中；
    # new_parsed 记录 leftover 里已经过完整行解析的字节数（残缺行留给下次）
    new_parsed = 0
    if partial_bytes is not None:
        new_parsed = len(leftover) - len(partial_bytes)
    return records, leftover, new_parsed, partial_bytes, partial_offset


def validate(record: Record, expect_suffix: int) -> None:
    """校验一条记录的完整结构：后缀、+ 行、序列与质量等长。"""
    head, seq, plus, qual, extra = _split_lines(record.raw)
    if extra:
        raise FastqFormatError("记录行数多于四行", offset=record.start)
    if head is None or seq is None or plus is None or qual is None:
        raise FastqFormatError("FASTQ 记录不足四行", offset=record.start)
    if not head.startswith(b"@") or len(head) < 2:
        raise FastqFormatError(f"记录头部非法: {head[:40]!r}", offset=record.start)
    if not head.endswith((f"/{expect_suffix}").encode()):
        raise FastqFormatError(
            f"R{expect_suffix} 文件中的读段名必须以 /{expect_suffix} 结尾: "
            f"{head[:40]!r}",
            offset=record.start,
        )
    if not plus.startswith(b"+"):
        raise FastqFormatError(f"第三行必须以 '+' 开头: {plus[:40]!r}", offset=record.start)
    if len(seq) != len(qual):
        raise FastqFormatError(
            f"序列长度({len(seq)})与质量串长度({len(qual)})不等: "
            f"{head[:40]!r}",
            offset=record.start,
        )


def _split_lines(raw: bytes):
    parts = raw.split(b"\n")
    # 末尾换行使 split 最后产生一个空元素，丢弃它
    if parts and parts[-1] == b"":
        parts = parts[:-1]
    head = parts[0] if len(parts) > 0 else None
    seq = parts[1] if len(parts) > 1 else None
    plus = parts[2] if len(parts) > 2 else None
    qual = parts[3] if len(parts) > 3 else None
    extra = len(parts) > 4
    return head, seq, plus, qual, extra
