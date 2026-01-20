use serde::Serialize;
use std::env;
use std::io::{self, BufRead, BufReader, Write};
use std::process::{Command, ExitStatus, Stdio};
use std::sync::mpsc::{self, Sender};
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Serialize)]
struct LogRecord {
    stream: String,
    line: String,
    timestamp_ms: u128,
}

fn strip_line_endings(mut line: String) -> String {
    while line.ends_with(['\n', '\r']) {
        line.pop();
    }
    line
}

fn line_bytes_to_string(bytes: &[u8]) -> String {
    strip_line_endings(String::from_utf8_lossy(bytes).to_string())
}

fn unix_timestamp_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time went backwards")
        .as_millis()
}

fn unix_timestamp_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time went backwards")
        .as_secs()
}

fn get_env_or_default(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_string())
}

fn build_output_filename(prefix: &str, hostname: &str, unix_ts: u64) -> String {
    let rank = get_env_or_default("RANK", "0");
    let local_rank = get_env_or_default("LOCAL_RANK", "0");
    let world_size = get_env_or_default("WORLD_SIZE", "1");

    format!(
        "{prefix}_{rank}_{local_rank}_{world_size}_{hostname}_{unix_ts}.jsonl.zst"
    )
}

fn spawn_reader_thread<R: io::Read + Send + 'static>(
    reader: R,
    stream_name: &'static str,
    tx: Sender<LogRecord>,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let mut buf_reader = BufReader::new(reader);
        let mut output = if stream_name == "stdout" {
            io::stdout().lock()
        } else {
            io::stderr().lock()
        };
        let mut buffer = Vec::new();
        loop {
            buffer.clear();
            let bytes_read = match buf_reader.read_until(b'\n', &mut buffer) {
                Ok(0) => break,
                Ok(n) => n,
                Err(err) => {
                    eprintln!("failed reading {stream_name}: {err}");
                    break;
                }
            };
            if bytes_read == 0 {
                break;
            }
            if let Err(err) = output.write_all(&buffer) {
                eprintln!("failed writing {stream_name}: {err}");
            }
            if let Err(err) = output.flush() {
                eprintln!("failed flushing {stream_name}: {err}");
            }
            let record = LogRecord {
                stream: stream_name.to_string(),
                line: line_bytes_to_string(&buffer),
                timestamp_ms: unix_timestamp_ms(),
            };
            if tx.send(record).is_err() {
                break;
            }
        }
    })
}

fn run(prefix: &str, command: &[String]) -> io::Result<ExitStatus> {
    let hostname = get_env_or_default("HOSTNAME", "unknown");
    let output_filename = build_output_filename(prefix, &hostname, unix_timestamp_secs());
    let file = std::fs::File::create(&output_filename)?;
    let mut encoder = zstd::Encoder::new(file, 10)?;

    let mut child = Command::new(&command[0])
        .args(&command[1..])
        .stdin(Stdio::inherit())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;

    let stdout = child.stdout.take().expect("stdout piped");
    let stderr = child.stderr.take().expect("stderr piped");

    let (tx, rx) = mpsc::channel();
    let stdout_handle = spawn_reader_thread(stdout, "stdout", tx.clone());
    let stderr_handle = spawn_reader_thread(stderr, "stderr", tx);

    for record in rx {
        serde_json::to_writer(&mut encoder, &record)?;
        encoder.write_all(b"\n")?;
    }

    let status = child.wait()?;
    let _ = stdout_handle.join();
    let _ = stderr_handle.join();
    let mut file = encoder.finish()?;
    file.flush()?;
    Ok(status)
}

fn parse_args() -> Result<(String, Vec<String>), String> {
    let mut args = env::args().skip(1).collect::<Vec<_>>();
    let separator_pos = args.iter().position(|arg| arg == "--");
    let Some(separator_pos) = separator_pos else {
        return Err("missing -- separator".to_string());
    };
    if separator_pos == 0 {
        return Err("missing FILE_PREFIX_TO_DUMP".to_string());
    }
    let prefix = args.remove(0);
    args.remove(separator_pos - 1);
    if args.is_empty() {
        return Err("missing underlying program".to_string());
    }
    Ok((prefix, args))
}

fn main() {
    let (prefix, command) = match parse_args() {
        Ok(values) => values,
        Err(message) => {
            eprintln!(
                "Error: {message}\nUsage: log_tee FILE_PREFIX_TO_DUMP -- underlying_program ..."
            );
            std::process::exit(2);
        }
    };

    match run(&prefix, &command) {
        Ok(status) => std::process::exit(status.code().unwrap_or(1)),
        Err(err) => {
            eprintln!("log_tee failed: {err}");
            std::process::exit(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strip_line_endings_removes_crlf() {
        let line = "hello world\r\n".to_string();
        assert_eq!(strip_line_endings(line), "hello world");
    }

    #[test]
    fn line_bytes_to_string_keeps_content() {
        let line = b"hello\n";
        assert_eq!(line_bytes_to_string(line), "hello");
    }

    #[test]
    fn build_output_filename_uses_env_defaults() {
        env::remove_var("RANK");
        env::remove_var("LOCAL_RANK");
        env::remove_var("WORLD_SIZE");
        let filename = build_output_filename("prefix", "host", 1234);
        assert_eq!(filename, "prefix_0_0_1_host_1234.jsonl.zst");
    }

    #[test]
    fn build_output_filename_reads_env() {
        env::set_var("RANK", "2");
        env::set_var("LOCAL_RANK", "3");
        env::set_var("WORLD_SIZE", "4");
        let filename = build_output_filename("prefix", "host", 42);
        assert_eq!(filename, "prefix_2_3_4_host_42.jsonl.zst");
    }
}
