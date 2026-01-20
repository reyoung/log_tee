# log_tee

`log_tee` 是一个小巧的命令行包装器，**在终端实时输出子进程 stdout/stderr** 的同时，**将结构化日志持久化为 JSONL 文件**，并支持自动切分与压缩。

## 功能特点

- **终端输出不丢失**：stdout/stderr 会继续显示在终端，同时写入日志文件。
- **结构化 JSONL**：每行都是包含 `stream`、`line`、`timestamp_ms` 的 JSON 对象。
- **确定性的文件命名**：包含分布式环境变量（`RANK`、`LOCAL_RANK`、`WORLD_SIZE`）、主机名和启动时间戳。
- **自动切分**：单个日志文件达到约 200MB 后自动切分，并把旧文件压缩为 `.zst`。
- **优雅终止（Unix）**：捕获 `SIGTERM`，转发给子进程；超时后发送 `SIGKILL`。

## 安装

```bash
cargo build --release
```

生成的可执行文件位于 `target/release/log_tee`。

## 使用方法

```bash
log_tee FILE_PREFIX_TO_DUMP [--sigkill-after-secs SECS] -- underlying_program [args...]
```

### 示例

```bash
log_tee /var/log/my_job/logs --sigkill-after-secs 10 -- bash -c 'echo hello; echo oops 1>&2'
```

## 日志文件命名

输出文件命名格式为：

```
{prefix}_{rank}_{local_rank}_{world_size}_{hostname}_{unix_ts}.{index}.jsonl
```

切分后会把旧文件压缩为 `.jsonl.zst`。

示例：

```
/var/log/my_job/logs_0_0_1_myhost_1715000000.0.jsonl
/var/log/my_job/logs_0_0_1_myhost_1715000000.0.jsonl.zst
```

## 日志记录格式

JSONL 中每行示例：

```json
{"stream":"stdout","line":"hello","timestamp_ms":1715000123456}
```

- `stream`: `stdout` 或 `stderr`
- `line`: 去除末尾换行的内容
- `timestamp_ms`: 毫秒级 unix 时间戳

## 环境变量

- `RANK`、`LOCAL_RANK`、`WORLD_SIZE`：用于分布式作业的文件命名。
- `HOSTNAME`：用于文件命名（默认为 `unknown`）。

## 退出码

`log_tee` 会返回被包裹命令的退出码，如出现内部错误则返回 `1`。
