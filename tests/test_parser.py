from collector.parser import FastqFormatError, split_fastq, validate
from collector.service import Record


def rec(seq_no: int, side: int, seq: str = "ACGT", qual: str = "IIII") -> bytes:
    return f"@seq{seq_no}/{side}\n{seq}\n+\n{qual}\n".encode()


def test_split_complete_records_with_absolute_offsets():
    buf = rec(1, 1) + rec(2, 1)
    records, leftover, parsed, partial, partial_off = split_fastq(buf, 0, 100)
    assert len(records) == 2
    assert records[0].name == "seq1"
    assert records[0].start == 100
    assert records[0].end == 100 + len(rec(1, 1))
    assert records[1].start == 100 + len(rec(1, 1))
    assert leftover == b"" and parsed == 0
    assert partial is None


def test_split_keeps_half_written_record_and_line():
    full = rec(1, 1)
    buf = full + b"@seq2/1\nACGT\n+\n"  # 缺第四行，但已有行都带换行
    records, leftover, parsed, partial, partial_off = split_fastq(buf, 0, 0)
    assert [r.name for r in records] == ["seq1"]
    assert leftover == b"@seq2/1\nACGT\n+\n"
    assert partial is None  # 末尾有换行，不算"未写完的行"

    # 最后一行连换行都没有 -> 报告 partial
    buf2 = full + b"@seq2/1\nACGT\n+\nIII"
    records, leftover, parsed, partial, partial_off = split_fastq(buf2, 0, 0)
    assert len(records) == 1
    assert partial == b"III"
    assert partial_off == len(full) + len(b"@seq2/1\nACGT\n+\n")


def test_split_incremental_feeds():
    """模拟字节一段段到达：任何时刻都不会产生错误配对。"""
    full = rec(1, 1) + rec(2, 1)
    buffer = bytearray()
    parsed = 0
    base = 0
    all_records = []
    for i in range(0, len(full), 7):  # 7 字节一切，故意切到行中间
        buffer.extend(full[i:i + 7])
        recs, leftover, parsed, _, _ = split_fastq(bytes(buffer), parsed, base)
        all_records.extend(recs)
        base += len(buffer) - len(leftover)
        buffer = bytearray(leftover)
    assert [r.name for r in all_records] == ["seq1", "seq2"]
    assert buffer == b""


def test_split_bad_header_reports_offset():
    buf = rec(1, 1) + b"NOAT\nACGT\n+\nIIII\n"
    try:
        split_fastq(buf, 0, 0)
    except FastqFormatError as exc:
        assert exc.offset == len(rec(1, 1))
    else:
        raise AssertionError("应当抛出结构错误")


def test_validate_suffix_and_length():
    good = Record(name="seq1", raw=rec(1, 1), start=0, end=len(rec(1, 1)))
    validate(good, 1)

    bad_suffix = Record(name="x", raw=b"@x/2\nACGT\n+\nIIII\n", start=0, end=16)
    try:
        validate(bad_suffix, 1)
    except FastqFormatError:
        pass
    else:
        raise AssertionError("R1 文件出现 /2 应报错")

    bad_len = Record(name="x", raw=b"@x/1\nACG\n+\nIIII\n", start=0, end=15)
    try:
        validate(bad_len, 1)
    except FastqFormatError as exc:
        assert "长度" in str(exc)
    else:
        raise AssertionError("序质长度不等应报错")


def test_validate_plus_line():
    bad = Record(name="x", raw=b"@x/1\nACGT\nX\nIIII\n", start=0, end=15)
    try:
        validate(bad, 1)
    except FastqFormatError:
        pass
    else:
        raise AssertionError("第三行不是 + 开头应报错")
