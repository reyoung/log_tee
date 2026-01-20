# log_tee

`log_tee` is a small CLI wrapper that **streams a command's stdout/stderr to the terminal** while **persisting structured JSONL logs** to disk with automatic rotation and compression.

## Features

- **Tee stdout/stderr**: output is still shown in the terminal while being recorded.
- **Structured JSONL**: each line is a JSON object with `stream`, `line`, and `timestamp_ms` fields.
- **Deterministic file naming**: includes distributed env vars (`RANK`, `LOCAL_RANK`, `WORLD_SIZE`), hostname, and a start timestamp.
- **Automatic rotation**: when a log reaches ~200 MB, it rotates and compresses the previous file into `.zst`.
- **Graceful shutdown handling (Unix)**: forwards `SIGTERM` to the child and sends `SIGKILL` after a configurable timeout.

## Installation

```bash
cargo build --release
```

The binary will be at `target/release/log_tee`.

## Usage

```bash
log_tee FILE_PREFIX_TO_DUMP [--sigkill-after-secs SECS] -- underlying_program [args...]
```

### Example

```bash
log_tee /var/log/my_job/logs --sigkill-after-secs 10 -- bash -c 'echo hello; echo oops 1>&2'
```

## Log file naming

Files are written as:

```
{prefix}_{rank}_{local_rank}_{world_size}_{hostname}_{unix_ts}.{index}.jsonl
```

When rotated, the previous segment is compressed to `.jsonl.zst`.

Example:

```
/var/log/my_job/logs_0_0_1_myhost_1715000000.0.jsonl
/var/log/my_job/logs_0_0_1_myhost_1715000000.0.jsonl.zst
```

## Log record format

Each line in the JSONL file looks like:

```json
{"stream":"stdout","line":"hello","timestamp_ms":1715000123456}
```

- `stream`: `stdout` or `stderr`
- `line`: line content without trailing newline
- `timestamp_ms`: unix timestamp in milliseconds

## Environment variables

- `RANK`, `LOCAL_RANK`, `WORLD_SIZE`: included in file names for multi-process jobs.
- `HOSTNAME`: used in file names (falls back to `unknown`).

## Exit codes

`log_tee` exits with the same exit code as the wrapped command, or `1` on internal error.
