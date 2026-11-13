# 双端 FASTQ 增量采集服务（FastAPI + SQLite）

测序中心数据员从仪器中转站接收持续追加的双端读段（`R1.fastq` / `R2.fastq`），
按读段名配对后交给下游比对。本项目提供：

- **采集端（collector）**：FastAPI + SQLite，通过 HTTP Range 增量拉取、
  四行 FASTQ 校验、按名称配对、字节级断点续采，全部配对完成后可下载 R1/R2；
- **仪器中转站（relay）**：本地模拟服务，支持 HTTP Range、分段追加、
  单端（R2）延迟写入、完成标志 `DONE` 与可控制的连接中断窗口；
- **一键演示（demo）**：自动建场景（含中断）、反复触发采集、下载并逐字节校验。

## 目录结构

```
.
├── collector/            # 采集端
│   ├── app.py            # REST API（采集/进度/下载/健康检查）
│   ├── service.py        # Range 续采 + 配对状态机 + 终态校验
│   ├── parser.py         # 字节级 FASTQ 解析与结构校验
│   └── db.py             # SQLite 模式与单事务配对提交
├── relay/                # 模拟仪器中转站
│   └── app.py            # Range 文件服务 + 追加写线程 + 中断控制
├── scripts/demo.py       # 端到端演示脚本
├── tests/                # 解析单元测试 + 真实子进程集成测试
├── Dockerfile            # 通用镜像（APP_MODULE 选择入口）
├── docker-compose.yml    # relay / collector / demo 三服务
└── requirements*.txt
```

## 快速开始（Docker Compose）

```bash
# 启动两个常驻服务（collector 在 8000，relay 在 8001）
docker compose up -d --build

# 运行端到端演示：自动建含中断的场景、增量采集、下载校验
docker compose --profile demo run --rm demo

# 查看日志
docker compose logs -f collector
```

也可手动操作：

```bash
# 1) 在中转站创建一个分段追加、R2 滞后、带连接中断的场景
curl -X POST http://localhost:8001/runs -H 'content-type: application/json' -d '{
  "run_id": "runA", "reads": 20, "append_delay": 0.2,
  "r2_extra_delay": 0.4, "chunk": 2,
  "interrupt_at_second": 1.0, "interrupt_duration": 2.0
}'

# 2) 提交采集（可任意重复调用，幂等）
curl -X POST http://localhost:8000/collect/runs -H 'content-type: application/json' \
  -d '{"run_id": "runA", "base_url": "http://relay:8000/runs/runA"}'

# 3) 查询进度
curl http://localhost:8000/collect/runs/runA

# 4) 全部配对（status=completed）后下载
curl -OJ http://localhost:8000/collect/runs/runA/R1
curl -OJ http://localhost:8000/collect/runs/runA/R2
```

> 在宿主机直接调用且不经过 compose 网络时，`base_url` 用
> `http://localhost:8001/runs/runA`。

## API 一览

### 采集端 `:8000`

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/collect/runs` | 提交/触发一次增量采集，body：`{"run_id","base_url"}`，幂等可重复 |
| GET | `/collect/runs` | 列出全部运行 |
| GET | `/collect/runs/{id}` | 查询状态、两端字节进度、配对数、错误详情 |
| GET | `/collect/runs/{id}/R1` `/R2` | 下载已配对 FASTQ，**仅 `completed` 允许**，否则 409 |
| GET | `/healthz` | 存活探针 |

进度响应示例：

```json
{
  "run_id": "runA",
  "base_url": "http://relay:8000/runs/runA",
  "status": "running",
  "r1_offset": 320,
  "r2_offset": 280,
  "pairs": 7,
  "error": null,
  "notices": ["R1 源站连接中断（HTTP 503），保留断点 440，等待重试"]
}
```

状态取值：`running`（采集中/等待追加）、`completed`（全部配对）、
`error`（结构错误、名称不匹配、完成后残条或孤读）。

### 中转站 `:8001`

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/runs` | 按场景启动后台追加任务（延迟、块大小、中断窗口均可配） |
| GET | `/runs/{id}/status` | 查看追加进度与中断窗口状态 |
| POST | `/runs/{id}/interrupt` | 手动打开中断窗口（body：`{"duration": 3}`） |
| POST | `/runs/{id}/resume` | 提前关闭中断窗口 |
| GET/HEAD | `/runs/{id}/R1.fastq` `/R2.fastq` `/DONE` | 文件服务，支持 `Range: bytes=a-b`（206/416） |

## 数据格式与配对规则

- 仅处理**未压缩** FASTQ，每条记录严格四行：
  `@名称` / 序列 / `+` / 质量串；
- R1 名称以 `/1` 结尾、R2 以 `/2` 结尾，**去掉后缀后名称一致**才配对；
- 序列与质量串必须等长，第三行必须以 `+` 开头，否则判定结构错误；
- 末条未写完（残行）或另一端滞后时**等待续采**，不产生半对；
- 源站 `DONE` 出现后，若仍有残条（`residual_records`）或孤读
  （`orphan_read`），进入 `error` 终态并回报两端字节位置；只有全部
  配对成功才置 `completed`，此时才允许下载。

## 不漏对、不重对的设计保证

1. **单事务提交边界**：每配成一对，在**同一个 SQLite 事务**里写入
   配对内容（`pairs` 表）并前推两端字节偏移（`runs` 表）。事务失败
   整体回滚，不存在"内容落库但偏移没更新"或反之的中间态。
2. **Range 断点续采**：下一次请求的起点 = 已提交偏移 + 内存残条长度；
   断线（503/连接重置/网络异常）只记录 `notices`，状态保持 `running`，
   已收字节保留，重连后从断点继续。
3. **重启恢复**：进程重启后内存状态清空，从 SQLite 中已提交的两端
   偏移重新发起 Range；已配对记录因唯一约束
   （`UNIQUE(run_id,name)` + 顺序号主键）不会重复。
4. **完成前必须确认 EOF**：源站出现 `DONE` 后，只有两端都收到明确的
   EOF 信号（416/404 且无传输错误）才做终态校验，避免把"还没拉到的
   完整记录"误判成残条。
5. **Range 一致性保护**：若源站在非零偏移处返回 200 而非 206（不支持
   Range，可能发生字节改写），直接进入 `error`，拒绝继续采集。

## 测试

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements-dev.txt
python -m pytest tests/ -q
```

测试包含 16 个用例：解析器偏移/残行/结构校验单元测试，以及真实
uvicorn 子进程下的端到端集成测试——增量配对、R2 滞后、连接中断续采、
**采集进程重启后从 SQLite 断点恢复**、名称不匹配/结构错误/残条/孤读
的错误终态与两端位置回报、Range 206/416、下载门控等。

## 环境变量

| 变量 | 服务 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `COLLECTOR_DB` | collector | `/data/collector/collector.db` | SQLite 文件路径 |
| `COLLECTOR_TIMEOUT` | collector | `10` | 单次 HTTP 超时秒数 |
| `RELAY_DATA` | relay | `/data/relay` | 运行目录根路径 |
| `RELAY_URL` / `COLLECTOR_URL` / `RUN_ID` | demo | 见 `scripts/demo.py` | 演示目标 |
