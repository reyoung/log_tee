use serde::Serialize;
use std::env;
use std::io::{self, BufRead, BufReader, Write};
use std::path::Path;
use std::process::{Command, ExitStatus, Stdio};
use std::sync::mpsc::{self, Sender};
use std::thread;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

#[cfg(unix)]
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

#[cfg(unix)]
static TERMINATE: AtomicBool = AtomicBool::new(false);
#[cfg(unix)]
static CHILD_PID: AtomicU32 = AtomicU32::new(0);

#[cfg(unix)]
extern "C" fn handle_sigterm(_: libc::c_int) {
    TERMINATE.store(true, Ordering::SeqCst);
}

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

fn build_output_filename(
    prefix: &str,
    hostname: &str,
    unix_ts: u64,
    index: u64,
    compress: bool,
) -> String {
    let rank = get_env_or_default("RANK", "0");
    let local_rank = get_env_or_default("LOCAL_RANK", "0");
    let world_size = get_env_or_default("WORLD_SIZE", "1");

    if compress {
        format!(
            "{prefix}_{rank}_{local_rank}_{world_size}_{hostname}_{unix_ts}.{index}.jsonl.zst"
        )
    } else {
        format!("{prefix}_{rank}_{local_rank}_{world_size}_{hostname}_{unix_ts}.{index}.jsonl")
    }
}

fn ensure_output_directory(path: &str) -> io::Result<()> {
    let parent = Path::new(path).parent();
    if let Some(parent) = parent {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)?;
        }
    }
    Ok(())
}

fn spawn_reader_thread<R: io::Read + Send + 'static>(
    reader: R,
    stream_name: &'static str,
    tx: Sender<LogRecord>,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let mut buf_reader = BufReader::new(reader);
        let mut output: Box<dyn Write + Send> = if stream_name == "stdout" {
            Box::new(io::stdout())
        } else {
            Box::new(io::stderr())
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

const MAX_OUTPUT_BYTES: u64 = 200 * 1024 * 1024;

struct RotatingWriter {
    prefix: String,
    hostname: String,
    start_ts: u64,
    index: u64,
    writer: io::BufWriter<std::fs::File>,
    bytes_written: u64,
}

impl RotatingWriter {
    fn new(prefix: &str, hostname: &str, start_ts: u64) -> io::Result<Self> {
        let index = 0;
        let filename = build_output_filename(prefix, hostname, start_ts, index, false);
        ensure_output_directory(&filename)?;
        let file = std::fs::File::create(&filename)?;
        Ok(Self {
            prefix: prefix.to_string(),
            hostname: hostname.to_string(),
            start_ts,
            index,
            writer: io::BufWriter::new(file),
            bytes_written: 0,
        })
    }

    fn write_record(&mut self, record: &LogRecord) -> io::Result<()> {
        let mut payload = serde_json::to_vec(record)?;
        payload.push(b'\n');
        self.writer.write_all(&payload)?;
        self.bytes_written += payload.len() as u64;
        if self.bytes_written >= MAX_OUTPUT_BYTES {
            self.rotate()?;
        }
        Ok(())
    }

    fn rotate(&mut self) -> io::Result<()> {
        self.writer.flush()?;
        let current_index = self.index;
        let plain_path = build_output_filename(
            &self.prefix,
            &self.hostname,
            self.start_ts,
            current_index,
            false,
        );
        self.index += 1;
        let next_path = build_output_filename(
            &self.prefix,
            &self.hostname,
            self.start_ts,
            self.index,
            false,
        );
        ensure_output_directory(&next_path)?;
        let new_file = std::fs::File::create(&next_path)?;
        let old_writer = std::mem::replace(&mut self.writer, io::BufWriter::new(new_file));
        drop(old_writer);
        self.bytes_written = 0;
        compress_file(&plain_path)?;
        Ok(())
    }

    fn finish(mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

fn compress_file(path: &str) -> io::Result<()> {
    let compressed_path = format!("{path}.zst");
    let mut input = std::fs::File::open(path)?;
    let output = std::fs::File::create(&compressed_path)?;
    let mut encoder = zstd::Encoder::new(output, 10)?;
    io::copy(&mut input, &mut encoder)?;
    let mut output = encoder.finish()?;
    output.flush()?;
    std::fs::remove_file(path)?;
    Ok(())
}

fn run(
    prefix: &str,
    command: &[String],
    sigkill_after: std::time::Duration,
) -> io::Result<ExitStatus> {
    let hostname = get_env_or_default("HOSTNAME", "unknown");
    let start_ts = unix_timestamp_secs();
    let mut output = RotatingWriter::new(prefix, &hostname, start_ts)?;
    #[cfg(unix)]
    {
        let signal_result =
            unsafe { libc::signal(libc::SIGTERM, handle_sigterm as libc::sighandler_t) };
        if signal_result == libc::SIG_ERR {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "failed to install SIGTERM handler",
            ));
        }
    }

    let mut child = Command::new(&command[0])
        .args(&command[1..])
        .stdin(Stdio::inherit())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    #[cfg(unix)]
    CHILD_PID.store(child.id(), Ordering::SeqCst);

    let stdout = child.stdout.take().expect("stdout piped");
    let stderr = child.stderr.take().expect("stderr piped");

    let (tx, rx) = mpsc::channel();
    let stdout_handle = spawn_reader_thread(stdout, "stdout", tx.clone());
    let stderr_handle = spawn_reader_thread(stderr, "stderr", tx);

    #[cfg(unix)]
    let mut sent_sigterm = false;
    #[cfg(unix)]
    let mut sent_sigkill = false;
    #[cfg(unix)]
    let mut sigterm_sent_at = None::<Instant>;
    loop {
        match rx.recv_timeout(std::time::Duration::from_millis(100)) {
            Ok(record) => {
                output.write_record(&record)?;
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                #[cfg(unix)]
                {
                    if TERMINATE.load(Ordering::SeqCst) {
                        let pid = CHILD_PID.load(Ordering::SeqCst);
                        if pid != 0 {
                            if !sent_sigterm {
                                unsafe {
                                    libc::kill(pid as i32, libc::SIGTERM);
                                }
                                sent_sigterm = true;
                                sigterm_sent_at = Some(Instant::now());
                            } else if !sent_sigkill {
                                if let Some(sent_at) = sigterm_sent_at {
                                    if sent_at.elapsed() >= sigkill_after {
                                        unsafe {
                                            libc::kill(pid as i32, libc::SIGKILL);
                                        }
                                        sent_sigkill = true;
                                    }
                                }
                            }
                        }
                    }
                }
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        }
    }

    let status = child.wait()?;
    let _ = stdout_handle.join();
    let _ = stderr_handle.join();
    output.finish()?;
    Ok(status)
}

fn parse_args() -> Result<(String, std::time::Duration, Vec<String>), String> {
    let args = env::args().skip(1).collect::<Vec<_>>();
    let separator_pos = args.iter().position(|arg| arg == "--");
    let Some(separator_pos) = separator_pos else {
        return Err("missing -- separator".to_string());
    };
    if separator_pos == 0 {
        return Err("missing FILE_PREFIX_TO_DUMP".to_string());
    }
    let prefix = args[0].clone();
    let mut sigkill_after_secs = 20u64;
    let mut idx = 1;
    while idx < separator_pos {
        match args[idx].as_str() {
            "--sigkill-after-secs" => {
                let value = args
                    .get(idx + 1)
                    .ok_or_else(|| "missing value for --sigkill-after-secs".to_string())?;
                sigkill_after_secs = value
                    .parse::<u64>()
                    .map_err(|_| "invalid value for --sigkill-after-secs".to_string())?;
                idx += 2;
            }
            other => {
                return Err(format!("unknown argument: {other}"));
            }
        }
    }
    let command = args[separator_pos + 1..].to_vec();
    if command.is_empty() {
        return Err("missing underlying program".to_string());
    }
    Ok((
        prefix,
        std::time::Duration::from_secs(sigkill_after_secs),
        command,
    ))
}

fn main() {
    let (prefix, sigkill_after, command) = match parse_args() {
        Ok(values) => values,
        Err(message) => {
            eprintln!(
                "Error: {message}\nUsage: log_tee FILE_PREFIX_TO_DUMP [--sigkill-after-secs SECS] -- underlying_program ..."
            );
            std::process::exit(2);
        }
    };

    match run(&prefix, &command, sigkill_after) {
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
    fn build_output_filename_uses_index() {
        env::remove_var("RANK");
        env::remove_var("LOCAL_RANK");
        env::remove_var("WORLD_SIZE");
        let filename = build_output_filename("prefix", "host", 1234, 0, false);
        assert_eq!(filename, "prefix_0_0_1_host_1234.0.jsonl");
        let filename = build_output_filename("prefix", "host", 1234, 1, true);
        assert_eq!(filename, "prefix_0_0_1_host_1234.1.jsonl.zst");
    }

    #[test]
    fn build_output_filename_reads_env() {
        env::set_var("RANK", "2");
        env::set_var("LOCAL_RANK", "3");
        env::set_var("WORLD_SIZE", "4");
        let filename = build_output_filename("prefix", "host", 42, 0, true);
        assert_eq!(filename, "prefix_2_3_4_host_42.0.jsonl.zst");
        let filename = build_output_filename("prefix", "host", 42, 2, false);
        assert_eq!(filename, "prefix_2_3_4_host_42.2.jsonl");
    }

    #[test]
    fn ensure_output_directory_creates_parent_dirs() {
        let mut base = env::temp_dir();
        base.push(format!("log_tee_test_{}", unix_timestamp_ms()));
        let nested = base.join("subdir/log.jsonl.zst");
        ensure_output_directory(
            nested
                .to_str()
                .expect("temp path should be valid unicode"),
        )
        .expect("create dir");
        assert!(base.join("subdir").is_dir());
        let _ = std::fs::remove_dir_all(&base);
    }
}
