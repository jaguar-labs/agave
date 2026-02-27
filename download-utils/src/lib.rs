#![cfg(feature = "agave-unstable-api")]
pub use solana_file_download::DownloadProgressRecord;
use {
    agave_snapshots::{
        ArchiveFormat, SnapshotArchiveKind, ZstdConfig, paths as snapshot_paths,
        snapshot_hash::SnapshotHash,
    },
    log::*,
    solana_clock::Slot,
    solana_file_download::{DownloadProgressCallbackOption, download_file},
    solana_genesis_config::DEFAULT_GENESIS_ARCHIVE,
    solana_runtime::snapshot_utils,
    std::{
        fs,
        io::{Read as _, Seek, SeekFrom, Write},
        net::SocketAddr,
        num::NonZeroUsize,
        path::{Path, PathBuf},
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicU64, Ordering},
        },
        thread,
        time::{Duration, Instant},
    },
};

const PARALLEL_DOWNLOAD_CHUNK_BUF_SIZE: usize = 64 * 1024; // 64 KB
const PROGRESS_POLL_INTERVAL: Duration = Duration::from_millis(250);
const PROGRESS_NOTIFY_INTERVAL: Duration = Duration::from_secs(5);

struct ParallelDownloadConfig {
    num_chunks: usize,
    min_file_size_for_parallel: u64,
    progress_notify_interval: Duration,
}

impl Default for ParallelDownloadConfig {
    fn default() -> Self {
        Self {
            num_chunks: 8,
            min_file_size_for_parallel: 100 * 1024 * 1024, // 100 MB
            progress_notify_interval: PROGRESS_NOTIFY_INTERVAL,
        }
    }
}

/// Compute non-overlapping inclusive byte ranges for `num_chunks` over `content_length` bytes.
fn compute_chunk_ranges(content_length: u64, num_chunks: usize) -> Vec<(u64, u64)> {
    if content_length == 0 || num_chunks == 0 {
        return vec![];
    }
    let num_chunks = num_chunks.min(content_length as usize) as u64;
    let chunk_size = content_length / num_chunks;
    let remainder = content_length % num_chunks;
    let mut ranges = Vec::with_capacity(num_chunks as usize);
    let mut start = 0u64;
    for i in 0..num_chunks {
        let extra = if i < remainder { 1 } else { 0 };
        let end = start + chunk_size + extra - 1;
        ranges.push((start, end));
        start = end + 1;
    }
    ranges
}

/// Build the temp file path matching `solana-file-download`'s convention: `tmp-<filename>`.
fn temp_file_path(destination: &Path) -> PathBuf {
    let parent = destination.parent().unwrap_or(Path::new("."));
    let file_name = destination.file_name().unwrap().to_str().unwrap();
    parent.join(format!("tmp-{file_name}"))
}

/// Errors from `download_file_parallel`.
#[derive(Debug)]
enum ParallelDownloadError {
    /// Infrastructure failure — caller should fall back to single-stream.
    Fallback(String),
    /// Application-level abort (e.g. progress callback said "too slow") — no fallback.
    Aborted(String),
}

/// Download a file using parallel HTTP Range requests.
///
/// Returns `Ok(())` on success. On failure, cleans up the temp file and returns an error
/// indicating whether the caller should fall back to single-stream (`Fallback`) or propagate
/// the error as-is (`Aborted`).
fn download_file_parallel(
    url: &str,
    destination_file: &Path,
    _use_progress_bar: bool,
    progress_notify_callback: &mut DownloadProgressCallbackOption<'_>,
    config: &ParallelDownloadConfig,
) -> Result<(), ParallelDownloadError> {
    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(300))
        .build()
        .map_err(|e| ParallelDownloadError::Fallback(format!("Failed to build HTTP client: {e}")))?;

    // HEAD probe to get Content-Length and check Accept-Ranges.
    let head_resp = client
        .head(url)
        .send()
        .map_err(|e| ParallelDownloadError::Fallback(format!("HEAD request failed: {e}")))?;

    if !head_resp.status().is_success() {
        return Err(ParallelDownloadError::Fallback(format!(
            "HEAD returned status {}",
            head_resp.status()
        )));
    }

    let accepts_ranges = head_resp
        .headers()
        .get("accept-ranges")
        .and_then(|v| v.to_str().ok())
        .map(|v| v.contains("bytes"))
        .unwrap_or(false);

    if !accepts_ranges {
        return Err(ParallelDownloadError::Fallback(
            "Server does not support Range requests".to_string(),
        ));
    }

    let content_length = head_resp
        .headers()
        .get("content-length")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok())
        .ok_or_else(|| {
            ParallelDownloadError::Fallback("Missing or invalid Content-Length header".to_string())
        })?;

    if content_length < config.min_file_size_for_parallel {
        return Err(ParallelDownloadError::Fallback(format!(
            "File too small for parallel download ({content_length} bytes < {} bytes)",
            config.min_file_size_for_parallel
        )));
    }

    let ranges = compute_chunk_ranges(content_length, config.num_chunks);
    info!(
        "Starting parallel download of {} bytes in {} chunks from {}",
        content_length,
        ranges.len(),
        url
    );

    // Pre-allocate temp file.
    let temp_path = temp_file_path(destination_file);
    let temp_file = fs::File::create(&temp_path).map_err(|e| {
        ParallelDownloadError::Fallback(format!("Failed to create temp file: {e}"))
    })?;
    temp_file.set_len(content_length).map_err(|e| {
        let _ = fs::remove_file(&temp_path);
        ParallelDownloadError::Fallback(format!("Failed to pre-allocate temp file: {e}"))
    })?;
    drop(temp_file);

    // Shared state for progress and abort.
    let total_downloaded = Arc::new(AtomicU64::new(0));
    let abort_flag = Arc::new(AtomicBool::new(false));

    // Spawn chunk download threads.
    let url_owned = url.to_string();
    let temp_path_clone = temp_path.clone();
    let handles: Vec<_> = ranges
        .iter()
        .enumerate()
        .map(|(i, &(start, end))| {
            let url = url_owned.clone();
            let temp_path = temp_path_clone.clone();
            let downloaded = Arc::clone(&total_downloaded);
            let abort = Arc::clone(&abort_flag);

            thread::spawn(move || -> Result<(), String> {
                let client = reqwest::blocking::Client::builder()
                    .timeout(Duration::from_secs(300))
                    .build()
                    .map_err(|e| format!("Chunk {i}: failed to build client: {e}"))?;

                let resp = client
                    .get(&url)
                    .header("Range", format!("bytes={start}-{end}"))
                    .send()
                    .map_err(|e| format!("Chunk {i}: GET failed: {e}"))?;

                if resp.status().as_u16() != 206 {
                    return Err(format!(
                        "Chunk {i}: expected 206 Partial Content, got {}",
                        resp.status()
                    ));
                }

                let mut file = fs::OpenOptions::new()
                    .write(true)
                    .open(&temp_path)
                    .map_err(|e| format!("Chunk {i}: failed to open temp file: {e}"))?;

                file.seek(SeekFrom::Start(start))
                    .map_err(|e| format!("Chunk {i}: seek failed: {e}"))?;

                let mut reader = resp;
                let mut buf = vec![0u8; PARALLEL_DOWNLOAD_CHUNK_BUF_SIZE];
                let mut bytes_written = 0u64;
                let expected = end - start + 1;

                loop {
                    if abort.load(Ordering::Relaxed) {
                        return Err(format!("Chunk {i}: aborted"));
                    }

                    let n = reader
                        .read(&mut buf)
                        .map_err(|e| format!("Chunk {i}: read error: {e}"))?;
                    if n == 0 {
                        break;
                    }

                    file.write_all(&buf[..n])
                        .map_err(|e| format!("Chunk {i}: write error: {e}"))?;

                    bytes_written += n as u64;
                    downloaded.fetch_add(n as u64, Ordering::Relaxed);
                }

                if bytes_written != expected {
                    return Err(format!(
                        "Chunk {i}: expected {expected} bytes, got {bytes_written}"
                    ));
                }

                Ok(())
            })
        })
        .collect();

    // Main thread: monitor progress until all threads complete.
    let download_start = Instant::now();
    let mut last_notify_time = Instant::now();
    let mut notification_count = 0u64;
    let mut last_notify_bytes = 0u64;
    let mut aborted_by_callback = false;

    loop {
        thread::sleep(PROGRESS_POLL_INTERVAL);

        let current_bytes = total_downloaded.load(Ordering::Relaxed);
        let elapsed = download_start.elapsed();

        // Notify callback periodically.
        if last_notify_time.elapsed() >= config.progress_notify_interval {
            notification_count += 1;
            let last_elapsed = last_notify_time.elapsed();
            let bytes_since_last = current_bytes.saturating_sub(last_notify_bytes);
            let last_throughput =
                bytes_since_last as f32 / last_elapsed.as_secs_f32().max(0.001);
            let total_throughput =
                current_bytes as f32 / elapsed.as_secs_f32().max(0.001);
            let percentage_done = if content_length > 0 {
                (current_bytes as f32 / content_length as f32) * 100.0
            } else {
                100.0
            };
            let remaining_bytes = content_length.saturating_sub(current_bytes) as f32;
            let estimated_remaining_time = if last_throughput > 0.0 {
                remaining_bytes / last_throughput
            } else {
                f32::MAX
            };

            let progress = DownloadProgressRecord {
                elapsed_time: elapsed,
                last_elapsed_time: last_elapsed,
                last_throughput,
                total_throughput,
                total_bytes: content_length as usize,
                current_bytes: current_bytes as usize,
                percentage_done,
                estimated_remaining_time,
                notification_count,
            };

            if let Some(ref mut callback) = progress_notify_callback {
                if !callback(&progress) {
                    info!("Parallel download aborted by progress callback");
                    abort_flag.store(true, Ordering::Relaxed);
                    aborted_by_callback = true;
                    break;
                }
            }

            last_notify_time = Instant::now();
            last_notify_bytes = current_bytes;
        }

        // Check if all threads are done.
        if handles.iter().all(|h| h.is_finished()) {
            break;
        }
    }

    // Collect thread results.
    let mut errors = Vec::new();
    for (i, handle) in handles.into_iter().enumerate() {
        match handle.join() {
            Ok(Ok(())) => {}
            Ok(Err(e)) => errors.push(e),
            Err(_) => errors.push(format!("Chunk {i}: thread panicked")),
        }
    }

    // Handle abort.
    if aborted_by_callback {
        let _ = fs::remove_file(&temp_path);
        return Err(ParallelDownloadError::Aborted(
            "Download aborted by progress callback".to_string(),
        ));
    }

    // Handle chunk errors.
    if !errors.is_empty() {
        let _ = fs::remove_file(&temp_path);
        return Err(ParallelDownloadError::Fallback(format!(
            "Parallel download chunk errors: {}",
            errors.join("; ")
        )));
    }

    // Rename temp to destination.
    fs::rename(&temp_path, destination_file).map_err(|e| {
        let _ = fs::remove_file(&temp_path);
        ParallelDownloadError::Fallback(format!("Failed to rename temp file: {e}"))
    })?;

    info!(
        "Parallel download complete: {} bytes in {:.1}s",
        content_length,
        download_start.elapsed().as_secs_f32()
    );

    Ok(())
}

pub fn download_genesis_if_missing(
    rpc_addr: &SocketAddr,
    genesis_package: &Path,
    use_progress_bar: bool,
) -> Result<PathBuf, String> {
    if !genesis_package.exists() {
        let tmp_genesis_path = genesis_package.parent().unwrap().join("tmp-genesis");
        let tmp_genesis_package = tmp_genesis_path.join(DEFAULT_GENESIS_ARCHIVE);

        let _ignored = fs::remove_dir_all(&tmp_genesis_path);
        download_file(
            &format!("http://{rpc_addr}/{DEFAULT_GENESIS_ARCHIVE}"),
            &tmp_genesis_package,
            use_progress_bar,
            &mut None,
        )?;

        Ok(tmp_genesis_package)
    } else {
        Err("genesis already exists".to_string())
    }
}

/// Download a snapshot archive from `rpc_addr`.  Use `snapshot_kind` to specify downloading either
/// a full snapshot or an incremental snapshot.
pub fn download_snapshot_archive(
    rpc_addr: &SocketAddr,
    full_snapshot_archives_dir: &Path,
    incremental_snapshot_archives_dir: &Path,
    desired_snapshot_hash: (Slot, SnapshotHash),
    snapshot_kind: SnapshotArchiveKind,
    maximum_full_snapshot_archives_to_retain: NonZeroUsize,
    maximum_incremental_snapshot_archives_to_retain: NonZeroUsize,
    use_progress_bar: bool,
    progress_notify_callback: &mut DownloadProgressCallbackOption<'_>,
) -> Result<(), String> {
    snapshot_utils::purge_old_snapshot_archives(
        full_snapshot_archives_dir,
        incremental_snapshot_archives_dir,
        maximum_full_snapshot_archives_to_retain,
        maximum_incremental_snapshot_archives_to_retain,
    );

    let snapshot_archives_remote_dir =
        snapshot_paths::build_snapshot_archives_remote_dir(match snapshot_kind {
            SnapshotArchiveKind::Full => full_snapshot_archives_dir,
            SnapshotArchiveKind::Incremental(_) => incremental_snapshot_archives_dir,
        });
    fs::create_dir_all(&snapshot_archives_remote_dir).unwrap();

    for archive_format in [
        ArchiveFormat::TarZstd {
            config: ZstdConfig::default(),
        },
        ArchiveFormat::TarLz4,
    ] {
        let destination_path = match snapshot_kind {
            SnapshotArchiveKind::Full => snapshot_paths::build_full_snapshot_archive_path(
                &snapshot_archives_remote_dir,
                desired_snapshot_hash.0,
                &desired_snapshot_hash.1,
                archive_format,
            ),
            SnapshotArchiveKind::Incremental(base_slot) => {
                snapshot_paths::build_incremental_snapshot_archive_path(
                    &snapshot_archives_remote_dir,
                    base_slot,
                    desired_snapshot_hash.0,
                    &desired_snapshot_hash.1,
                    archive_format,
                )
            }
        };

        if destination_path.is_file() {
            return Ok(());
        }

        let url = format!(
            "http://{}/{}",
            rpc_addr,
            destination_path.file_name().unwrap().to_str().unwrap()
        );

        // Try parallel download first, fall back to single-stream on infrastructure failures.
        match download_file_parallel(
            &url,
            &destination_path,
            use_progress_bar,
            progress_notify_callback,
            &ParallelDownloadConfig::default(),
        ) {
            Ok(()) => return Ok(()),
            Err(ParallelDownloadError::Aborted(err)) => {
                // Application-level abort (e.g. too slow) — propagate without fallback.
                return Err(err);
            }
            Err(ParallelDownloadError::Fallback(err)) => {
                info!("Parallel download failed ({err}), falling back to single-stream");
                match download_file(
                    &url,
                    &destination_path,
                    use_progress_bar,
                    progress_notify_callback,
                ) {
                    Ok(()) => return Ok(()),
                    Err(err) => info!("{err}"),
                }
            }
        }
    }
    Err(format!(
        "Failed to download a snapshot archive for slot {} from {}",
        desired_snapshot_hash.0, rpc_addr
    ))
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        std::{
            io::{BufRead, BufReader},
            net::TcpListener,
        },
    };

    #[test]
    fn test_chunk_range_calculation() {
        // Basic case: 1000 bytes, 4 chunks
        let ranges = compute_chunk_ranges(1000, 4);
        assert_eq!(ranges.len(), 4);
        // Verify no gaps or overlaps and full coverage
        assert_eq!(ranges[0].0, 0);
        assert_eq!(ranges.last().unwrap().1, 999);
        for i in 1..ranges.len() {
            assert_eq!(ranges[i].0, ranges[i - 1].1 + 1);
        }

        // Verify total bytes covered
        let total: u64 = ranges.iter().map(|(s, e)| e - s + 1).sum();
        assert_eq!(total, 1000);

        // Remainder case: 1003 bytes, 4 chunks — first 3 get 251, last gets 250
        let ranges = compute_chunk_ranges(1003, 4);
        assert_eq!(ranges.len(), 4);
        assert_eq!(ranges[0].0, 0);
        assert_eq!(ranges.last().unwrap().1, 1002);
        let total: u64 = ranges.iter().map(|(s, e)| e - s + 1).sum();
        assert_eq!(total, 1003);

        // Edge case: file_size < num_chunks — should clamp to file_size chunks
        let ranges = compute_chunk_ranges(3, 8);
        assert_eq!(ranges.len(), 3);
        assert_eq!(ranges[0], (0, 0));
        assert_eq!(ranges[1], (1, 1));
        assert_eq!(ranges[2], (2, 2));

        // Edge case: 1 chunk
        let ranges = compute_chunk_ranges(500, 1);
        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0], (0, 499));

        // Edge case: 0 bytes
        let ranges = compute_chunk_ranges(0, 4);
        assert!(ranges.is_empty());

        // Edge case: 0 chunks
        let ranges = compute_chunk_ranges(100, 0);
        assert!(ranges.is_empty());
    }

    /// Minimal HTTP server for tests. Serves a fixed body with optional Range support.
    struct TestHttpServer {
        addr: SocketAddr,
        shutdown: Arc<AtomicBool>,
        handle: Option<thread::JoinHandle<()>>,
    }

    impl TestHttpServer {
        fn start(body: Vec<u8>, support_ranges: bool) -> Self {
            Self::start_with_delay(body, support_ranges, None)
        }

        fn start_with_delay(
            body: Vec<u8>,
            support_ranges: bool,
            chunk_write_delay: Option<Duration>,
        ) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();
            let shutdown = Arc::new(AtomicBool::new(false));
            let shutdown_clone = Arc::clone(&shutdown);

            listener.set_nonblocking(true).unwrap();

            let handle = thread::spawn(move || {
                while !shutdown_clone.load(Ordering::Relaxed) {
                    let stream = match listener.accept() {
                        Ok((stream, _)) => stream,
                        Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                            thread::sleep(Duration::from_millis(10));
                            continue;
                        }
                        Err(_) => break,
                    };

                    stream.set_nonblocking(false).unwrap();
                    let body = body.clone();

                    thread::spawn(move || {
                        let mut reader = BufReader::new(&stream);
                        let mut request_line = String::new();
                        if reader.read_line(&mut request_line).is_err() {
                            return;
                        }

                        // Read all headers
                        let mut range_header = None;
                        let is_head = request_line.starts_with("HEAD");
                        loop {
                            let mut line = String::new();
                            if reader.read_line(&mut line).is_err() {
                                return;
                            }
                            if line.trim().is_empty() {
                                break;
                            }
                            if line.to_lowercase().starts_with("range:") {
                                range_header =
                                    Some(line.split_once(':').unwrap().1.trim().to_string());
                            }
                        }

                        if is_head {
                            let mut response = format!(
                                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n",
                                body.len()
                            );
                            if support_ranges {
                                response.push_str("Accept-Ranges: bytes\r\n");
                            }
                            response.push_str("Connection: close\r\n\r\n");
                            let _ = std::io::Write::write_all(
                                &mut &stream,
                                response.as_bytes(),
                            );
                            return;
                        }

                        // GET request
                        if support_ranges {
                            if let Some(range) = range_header {
                                // Parse "bytes=start-end"
                                let range =
                                    range.trim_start_matches("bytes=");
                                let parts: Vec<&str> = range.split('-').collect();
                                let start: usize = parts[0].parse().unwrap();
                                let end: usize = parts[1].parse().unwrap();
                                let slice = &body[start..=end];

                                let response = format!(
                                    "HTTP/1.1 206 Partial Content\r\n\
                                     Content-Length: {}\r\n\
                                     Content-Range: bytes {}-{}/{}\r\n\
                                     Connection: close\r\n\r\n",
                                    slice.len(),
                                    start,
                                    end,
                                    body.len()
                                );
                                let _ = std::io::Write::write_all(
                                    &mut &stream,
                                    response.as_bytes(),
                                );
                                if let Some(delay) = chunk_write_delay {
                                    // Write in 1KB chunks with delay to simulate slow server.
                                    for chunk in slice.chunks(1024) {
                                        if std::io::Write::write_all(&mut &stream, chunk).is_err()
                                        {
                                            return;
                                        }
                                        thread::sleep(delay);
                                    }
                                } else {
                                    let _ = std::io::Write::write_all(&mut &stream, slice);
                                }
                                return;
                            }
                        }

                        // Full response
                        let response = format!(
                            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                            body.len()
                        );
                        let _ =
                            std::io::Write::write_all(&mut &stream, response.as_bytes());
                        let _ = std::io::Write::write_all(&mut &stream, &body);
                    });
                }
            });

            Self {
                addr,
                shutdown,
                handle: Some(handle),
            }
        }
    }

    impl Drop for TestHttpServer {
        fn drop(&mut self) {
            self.shutdown.store(true, Ordering::Relaxed);
            if let Some(h) = self.handle.take() {
                let _ = h.join();
            }
        }
    }

    #[test]
    fn test_download_file_parallel_fallback_no_range_support() {
        let body = vec![0u8; 1024];
        let server = TestHttpServer::start(body, false);
        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("testfile");

        let result = download_file_parallel(
            &format!("http://{}/testfile", server.addr),
            &dest,
            false,
            &mut None,
            &ParallelDownloadConfig {
                num_chunks: 2,
                min_file_size_for_parallel: 0,
                ..Default::default()
            },
        );

        assert!(matches!(result, Err(ParallelDownloadError::Fallback(_))));
        assert!(!dest.exists());
    }

    #[test]
    fn test_download_file_parallel_fallback_file_too_small() {
        let body = vec![42u8; 1024];
        let server = TestHttpServer::start(body, true);
        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("testfile");

        let result = download_file_parallel(
            &format!("http://{}/testfile", server.addr),
            &dest,
            false,
            &mut None,
            &ParallelDownloadConfig {
                num_chunks: 2,
                min_file_size_for_parallel: 100 * 1024 * 1024, // 100 MB
                ..Default::default()
            },
        );

        assert!(matches!(result, Err(ParallelDownloadError::Fallback(_))));
    }

    #[test]
    fn test_download_file_parallel_success() {
        // Create a known test body
        let body: Vec<u8> = (0..10_000u64)
            .flat_map(|i| i.to_le_bytes())
            .collect();
        let server = TestHttpServer::start(body.clone(), true);
        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("testfile");

        let result = download_file_parallel(
            &format!("http://{}/testfile", server.addr),
            &dest,
            false,
            &mut None,
            &ParallelDownloadConfig {
                num_chunks: 4,
                min_file_size_for_parallel: 0, // override for test
                ..Default::default()
            },
        );

        assert!(result.is_ok(), "download failed: {:?}", result.err());
        assert!(dest.exists());

        let downloaded = fs::read(&dest).unwrap();
        assert_eq!(downloaded.len(), body.len());
        assert_eq!(downloaded, body);
    }

    #[test]
    fn test_download_file_parallel_abort() {
        // Use a slow server so download threads are still running when progress fires.
        let body = vec![0u8; 200_000];
        let server =
            TestHttpServer::start_with_delay(body, true, Some(Duration::from_millis(100)));
        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("testfile");

        // Callback immediately returns false to abort.
        let mut callback: DownloadProgressCallbackOption<'_> =
            Some(Box::new(|_progress: &DownloadProgressRecord| false));

        let result = download_file_parallel(
            &format!("http://{}/testfile", server.addr),
            &dest,
            false,
            &mut callback,
            &ParallelDownloadConfig {
                num_chunks: 4,
                min_file_size_for_parallel: 0,
                // Fire progress callback as soon as possible.
                progress_notify_interval: Duration::from_millis(0),
            },
        );

        assert!(matches!(result, Err(ParallelDownloadError::Aborted(_))));
        // Temp file should be cleaned up.
        let temp = temp_file_path(&dest);
        assert!(!temp.exists(), "temp file was not cleaned up");
    }
}
