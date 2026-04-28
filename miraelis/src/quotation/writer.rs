use barter_data::event::{DataKind, MarketEvent};
use bytes::Bytes;
use chrono::Utc;
use crossbeam_channel::{self, TryRecvError};
use memmap2::{MmapMut, MmapOptions}; // 添加 MmapOptions
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    ptr,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
};
use tokio::{
    fs,
    io::AsyncWriteExt,
    time::{sleep, Duration},
};
use tracing::{info, warn};

#[derive(Debug, Clone)]
pub struct SymbolSpec {
    pub symbol: String,
    pub base: String,
    pub quote: String,
}

struct Chunk {
    buffer: Arc<MmapMut>,
    offset: AtomicUsize,
    max_size: usize,
}

unsafe impl Send for Chunk {}
unsafe impl Sync for Chunk {}

impl Chunk {
    fn new(max_size: usize) -> std::io::Result<Self> {
        let mmap = MmapOptions::new().len(max_size).map_anon().map_err(|err| {
            tracing::error!("mmap {} bytes failed with err:{}", max_size, err);
            err
        })?;

        Ok(Self {
            buffer: Arc::new(mmap),
            offset: AtomicUsize::new(0),
            max_size,
        })
    }

    fn push(&self, line: &Bytes) -> std::io::Result<()> {
        let len = line.len() + 1; // +1 for newline
        let current_offset = self.offset.load(Ordering::Relaxed);

        if current_offset + len > self.max_size {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                "Chunk is full",
            ));
        }

        // --- 关键部分：安全地写入数据 ---
        // 1. 计算目标地址
        let dst_ptr = self.buffer.as_ptr() as *mut u8; // 获取 MmapMut 内部数据的起始地址
        let write_ptr = unsafe { dst_ptr.add(current_offset) }; // 计算写入地址

        // 2. 执行内存拷贝
        // 这是 unsafe 的核心部分，我们必须确保：
        // - src_ptr 和 dst_ptr 指向有效的内存
        // - 内存区域不重叠（对于 copy_nonoverlapping）
        // - 写入大小不超过声明的长度
        let src_ptr = line.as_ptr();
        unsafe {
            ptr::copy_nonoverlapping(src_ptr, write_ptr, line.len()); // 拷贝数据
            *write_ptr.add(line.len()) = b'\n'; // 添加换行符
        }

        // 3. 更新偏移量
        self.offset.store(current_offset + len, Ordering::Release);

        Ok(())
    }

    fn clear(&self) -> std::io::Result<()> {
        // 重置偏移量即可，无需清零整个内存区域
        self.offset.store(0, Ordering::Release);
        Ok(())
    }

    fn get_data(&self) -> &[u8] {
        let current_offset = self.offset.load(Ordering::Acquire);
        // 安全地从 MmapMut 创建一个切片
        // 这是安全的，因为：
        // 1. self.buffer.as_ptr() 给出了有效的起始地址
        // 2. current_offset 是由我们自己通过原子操作维护的，不会越界
        // 3. MmapMut 的生命周期由 Arc 管理，只要 Chunk 存在，它就存在
        unsafe { std::slice::from_raw_parts(self.buffer.as_ptr(), current_offset) }
    }
}

/// 环形缓冲区
struct RingBuffer {
    chunks: Vec<Arc<Chunk>>, // 使用 Arc 包装 Chunk 以实现线程间安全共享
    current_chunk_index: usize,
}

impl RingBuffer {
    fn new(num_chunks: usize, chunk_size: usize) -> std::io::Result<Self> {
        let mut chunks = Vec::with_capacity(num_chunks);
        for _i in 0..num_chunks {
            let chunk = Chunk::new(chunk_size)?;
            chunks.push(Arc::new(chunk));
        }
        Ok(Self {
            chunks,
            current_chunk_index: 0,
        })
    }

    fn push(&mut self, line: &Bytes) -> std::io::Result<()> {
        let mut attempts = 0;
        let num_chunks = self.chunks.len();
        loop {
            match self.chunks[self.current_chunk_index].push(line) {
                // If successful, do nothing
                Ok(()) => return Ok(()),
                // If the chunk is full, clear it and rotate to the next chunk
                Err(e) if e.kind() == std::io::ErrorKind::WriteZero => {
                    // Clear the current chunk (this doesn't write to disk)
                    self.chunks[self.current_chunk_index].clear()?;
                    // Move to the next chunk
                    self.current_chunk_index = (self.current_chunk_index + 1) % num_chunks;
                    attempts += 1;
                    // If we've tried all chunks and none have space, return an error
                    if attempts >= num_chunks {
                        return Err(e);
                    }
                    // Otherwise, try pushing to the new chunk
                }
                // For other errors, propagate them up
                Err(e) => return Err(e),
            }
        }
    }
}

/// 每一个逻辑文件路径（如 `{dir}/trades.jsonl`）对应的分片状态。
/// 物理路径格式：`{dir}/{stem}_{date}_{idx:04}{ext}`
struct ShardState {
    /// 当前活跃日期字符串 ("YYYY-MM-DD")
    date_str: String,
    /// 当前分片序号（从 1 开始）
    shard_idx: u32,
    /// 当前分片已写入字节数
    current_bytes: u64,
    data_file: fs::File,
    /// 环形缓冲区
    ring_buffer: RingBuffer,
}

impl ShardState {
    fn new(
        date_str: String,
        shard_idx: u32,
        current_bytes: u64,
        data_file: fs::File,
    ) -> std::io::Result<Self> {
        Ok(Self {
            date_str,
            shard_idx,
            current_bytes,
            data_file,
            ring_buffer: RingBuffer::new(4, 1024 * 1024 * 200)?, // 4 chunks, 200MB each
        })
    }

    async fn flush(&mut self) -> std::io::Result<()> {
        // Iterate through the chunks and write their data to the file
        for chunk in &self.ring_buffer.chunks {
            let data = chunk.get_data();
            if !data.is_empty() {
                self.data_file.write_all(data).await?;
                // Update the current bytes count
                self.current_bytes += data.len() as u64;
            }
            // Clear the chunk after writing its data
            chunk.clear()?;
        }

        // Reset the ring buffer's current chunk index
        self.ring_buffer.current_chunk_index = 0;

        // Sync the file data to disk
        self.data_file.sync_data().await?;

        Ok(())
    }
}

struct WriteRequest {
    /// 逻辑路径，不含日期/序号后缀
    path: String,
    line: Bytes,
}

#[derive(Clone)]
pub struct AsyncRollbackWriter {
    tx: crossbeam_channel::Sender<MarketEvent<String, DataKind>>,
    output_dir: String,
    shutdown_flag: Arc<AtomicBool>,
}

impl AsyncRollbackWriter {
    /// `max_shard_bytes`: 单个分片文件的最大字节数，0 表示不限大小（仅按日期分片）。
    pub async fn start(
        buffer: usize,
        max_shard_bytes: u64,
        output_dir: &str,
    ) -> (Self, tokio::task::JoinHandle<()>) {
        let _ = fs::create_dir_all(output_dir).await;
        let (tx, rx) = crossbeam_channel::bounded::<MarketEvent<String, DataKind>>(buffer);
        let output_dir_clone = output_dir.to_string();
        let shutdown_flag = Arc::new(AtomicBool::new(false));

        let task = {
            let shutdown_flag = Arc::clone(&shutdown_flag);
            tokio::spawn(async move {
                let output_dir = output_dir_clone;
                // key = logical path
                let mut shards: HashMap<String, ShardState> = HashMap::new();

                let mut last_flush = std::time::Instant::now();
                loop {
                    // Check if shutdown flag is set
                    if shutdown_flag.load(Ordering::Relaxed) {
                        // Flush all remaining data
                        for shard in shards.values_mut() {
                            if let Err(e) = shard.flush().await {
                                warn!("Failed to flush shard during shutdown: {:?}", e);
                            }
                        }
                        break;
                    }

                    let event = match rx.try_recv() {
                        Ok(event) => event,
                        Err(TryRecvError::Empty) => {
                            // Check if it's time to flush the buffers
                            if last_flush.elapsed() > Duration::from_millis(100) {
                                // Flush every 100ms
                                for shard in shards.values_mut() {
                                    if let Err(e) = shard.flush().await {
                                        warn!("Failed to flush shard: {:?}", e);
                                    }
                                }
                                last_flush = std::time::Instant::now();
                            }
                            sleep(Duration::from_millis(2)).await;
                            continue;
                        }
                        Err(TryRecvError::Disconnected) => {
                            // Flush all remaining data
                            for shard in shards.values_mut() {
                                if let Err(e) = shard.flush().await {
                                    warn!("Failed to flush shard after disconnection: {:?}", e);
                                }
                            }
                            break;
                        }
                    };

                    // 处理事件并写入文件
                    let now = Utc::now().to_rfc3339();
                    let symbol = extract_symbol(&event.instrument);

                    match event.kind {
                        DataKind::Trade(trade) => {
                            let payload = serde_json::json!({
                                "time": now,
                                "instrument": event.instrument,
                                "symbol": symbol,
                                "exchange": event.exchange,
                                "kind": "trade",
                                "data": trade,
                            });
                            // tracing::debug!("recved: {}", payload);
                            let req = WriteRequest {
                                path: format!("{}/trades.jsonl", output_dir),
                                line: Bytes::from(serde_json::to_vec(&payload).unwrap_or_default()),
                            };
                            if let Err(e) = write_sharded(&req, &mut shards, max_shard_bytes).await
                            {
                                warn!("Failed to write trade event: {:?}", e);
                            }
                        }
                        DataKind::Candle(candle) => {
                            if candle.is_final {
                                let (kind, file_name) = if event.instrument.ends_with("|kline_1m") {
                                    ("kline_1m", "candles_1m.jsonl")
                                } else if event.instrument.ends_with("|kline_1h") {
                                    ("kline_1h", "candles_1h.jsonl")
                                } else {
                                    ("candle", "candles.jsonl")
                                };

                                let payload = serde_json::json!({
                                    "time": now,
                                    "instrument": event.instrument,
                                    "symbol": symbol,
                                    "exchange": event.exchange,
                                    "kind": kind,
                                    "data": candle,
                                });

                                let req = WriteRequest {
                                    path: format!("{}/{}", output_dir, file_name),
                                    line: Bytes::from(
                                        serde_json::to_vec(&payload).unwrap_or_default(),
                                    ),
                                };
                                if let Err(e) =
                                    write_sharded(&req, &mut shards, max_shard_bytes).await
                                {
                                    warn!("Failed to write candle event: {:?}", e);
                                }
                            }
                        }
                        DataKind::Liquidation(liquidation) => {
                            let payload = serde_json::json!({
                                "time": now,
                                "instrument": event.instrument,
                                "symbol": symbol,
                                "exchange": event.exchange,
                                "kind": "liquidation",
                                "data": liquidation,
                            });
                            let req = WriteRequest {
                                path: format!("{}/liquidations.jsonl", output_dir),
                                line: Bytes::from(serde_json::to_vec(&payload).unwrap_or_default()),
                            };
                            if let Err(e) = write_sharded(&req, &mut shards, max_shard_bytes).await
                            {
                                warn!("Failed to write liquidation event: {:?}", e);
                            }
                        }
                        _ => {}
                    }
                }

                // 刷写剩余
                while let Ok(event) = rx.try_recv() {
                    // 处理事件并写入文件
                    let now = Utc::now().to_rfc3339();
                    let symbol = extract_symbol(&event.instrument);

                    match event.kind {
                        DataKind::Trade(trade) => {
                            let payload = serde_json::json!({
                                "time": now,
                                "instrument": event.instrument,
                                "symbol": symbol,
                                "exchange": event.exchange,
                                "kind": "trade",
                                "data": trade,
                            });
                            let req = WriteRequest {
                                path: format!("{}/trades.jsonl", output_dir),
                                line: Bytes::from(serde_json::to_vec(&payload).unwrap_or_default()),
                            };
                            if let Err(e) = write_sharded(&req, &mut shards, max_shard_bytes).await
                            {
                                warn!("Failed to write trade event: {:?}", e);
                            }
                        }
                        DataKind::Candle(candle) => {
                            if candle.is_final {
                                let (kind, file_name) = if event.instrument.ends_with("|kline_1m") {
                                    ("kline_1m", "candles_1m.jsonl")
                                } else if event.instrument.ends_with("|kline_1h") {
                                    ("kline_1h", "candles_1h.jsonl")
                                } else {
                                    ("candle", "candles.jsonl")
                                };

                                let payload = serde_json::json!({
                                    "time": now,
                                    "instrument": event.instrument,
                                    "symbol": symbol,
                                    "exchange": event.exchange,
                                    "kind": kind,
                                    "data": candle,
                                });

                                let req = WriteRequest {
                                    path: format!("{}/{}", output_dir, file_name),
                                    line: Bytes::from(
                                        serde_json::to_vec(&payload).unwrap_or_default(),
                                    ),
                                };
                                if let Err(e) =
                                    write_sharded(&req, &mut shards, max_shard_bytes).await
                                {
                                    warn!("Failed to write candle event: {:?}", e);
                                }
                            }
                        }
                        DataKind::Liquidation(liquidation) => {
                            let payload = serde_json::json!({
                                "time": now,
                                "instrument": event.instrument,
                                "symbol": symbol,
                                "exchange": event.exchange,
                                "kind": "liquidation",
                                "data": liquidation,
                            });
                            let req = WriteRequest {
                                path: format!("{}/liquidations.jsonl", output_dir),
                                line: Bytes::from(serde_json::to_vec(&payload).unwrap_or_default()),
                            };
                            if let Err(e) = write_sharded(&req, &mut shards, max_shard_bytes).await
                            {
                                warn!("Failed to write liquidation event: {:?}", e);
                            }
                        }
                        _ => {}
                    }
                }
            })
        };

        (
            Self {
                tx,
                output_dir: output_dir.to_string(),
                shutdown_flag,
            },
            task,
        )
    }

    /// 处理 MarketEvent 并写入到对应的文件
    pub fn write_market_event(
        &self,
        event: MarketEvent<String, DataKind>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.tx
            .send(event)
            .map_err(|error| format!("async writer channel closed: {error}").into())
    }
}

/// 从 instrument 提取 symbol
fn extract_symbol(instrument: &str) -> String {
    instrument
        .split('|')
        .next()
        .unwrap_or(instrument)
        .to_uppercase()
}

/// 将一条 line 写入正确的分片，自动处理日期轮转和大小分片。
async fn write_sharded(
    req: &WriteRequest,
    shards: &mut HashMap<String, ShardState>,
    max_shard_bytes: u64,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let today = Utc::now().format("%Y-%m-%d").to_string();
    let need_new_shard = match shards.get(&req.path) {
        None => true,
        Some(s) => {
            s.date_str != today || (max_shard_bytes > 0 && s.current_bytes >= max_shard_bytes)
        }
    };

    if need_new_shard {
        // 启动后的首次写入要恢复到当天最新分片，而不是总是回到 `_0001`。
        let (physical, new_date, new_idx) = match shards.get(&req.path) {
            None => {
                let (physical, idx) = select_resume_shard_path(&req.path, &today, max_shard_bytes);
                (physical, today.clone(), idx)
            }
            Some(s) if s.date_str != today => {
                let physical = make_shard_path(&req.path, &today, 1u32);
                (physical, today.clone(), 1u32)
            }
            Some(s) => {
                let idx = s.shard_idx + 1;
                let physical = make_shard_path(&req.path, &today, idx);
                (physical, today.clone(), idx)
            }
        };
        match open_data_file(&physical).await {
            Ok(data_file) => {
                let current_bytes = data_file.metadata().await.map(|m| m.len()).unwrap_or(0);
                info!(
                    logical = %req.path,
                    physical = %physical,
                    shard_idx = new_idx,
                    "opened new shard file"
                );
                shards.insert(
                    req.path.clone(),
                    ShardState::new(new_date, new_idx, current_bytes, data_file)?,
                );
            }
            Err(error) => {
                warn!(path = %req.path, ?error, "open shard writer failed");
                return Ok(());
            }
        }
    }

    let Some(shard) = shards.get_mut(&req.path) else {
        return Ok(());
    };

    // 将数据推入环形缓冲区
    shard.ring_buffer.push(&req.line)?;

    Ok(())
}

/// 为当天已有分片选择恢复写入的物理路径。
/// - 若当天不存在分片：返回 `_0001`
/// - 若当天最新分片未达到大小上限：继续写该分片
/// - 若当天最新分片已达到大小上限：创建下一个分片
fn select_resume_shard_path(logical: &str, date_str: &str, max_shard_bytes: u64) -> (String, u32) {
    let logical_path = PathBuf::from(logical);
    let stem = logical_path
        .file_stem()
        .unwrap_or_default()
        .to_string_lossy()
        .into_owned();
    let ext = logical_path
        .extension()
        .map(|value| format!(".{}", value.to_string_lossy()))
        .unwrap_or_default();
    let prefix = format!("{stem}_{date_str}_");

    let search_dir = logical_path.parent().unwrap_or_else(|| Path::new("."));
    let mut latest_idx = 0u32;
    let mut latest_path = None::<String>;

    if let Ok(entries) = std::fs::read_dir(search_dir) {
        for entry in entries.flatten() {
            let file_name = entry.file_name();
            let file_name = file_name.to_string_lossy();

            if !file_name.starts_with(&prefix) || !file_name.ends_with(&ext) {
                continue;
            }

            let idx_part = &file_name[prefix.len()..file_name.len().saturating_sub(ext.len())];
            let Ok(idx) = idx_part.parse::<u32>() else {
                continue;
            };

            if idx > latest_idx {
                latest_idx = idx;
                latest_path = Some(entry.path().to_string_lossy().into_owned());
            }
        }
    }

    if let Some(existing_path) = latest_path {
        let current_size = std::fs::metadata(&existing_path)
            .map(|meta| meta.len())
            .unwrap_or(0);
        if max_shard_bytes == 0 || current_size < max_shard_bytes {
            return (existing_path, latest_idx);
        }

        let next_idx = latest_idx.saturating_add(1).max(1);
        return (make_shard_path(logical, date_str, next_idx), next_idx);
    }

    (make_shard_path(logical, date_str, 1), 1)
}

/// 将逻辑路径 + 日期 + 序号 拼成物理路径。
/// 例：`/data/trades.jsonl` + `2026-04-19` + 2 => `/data/trades_2026-04-19_0002.jsonl`
fn make_shard_path(logical: &str, date_str: &str, shard_idx: u32) -> String {
    let path = PathBuf::from(logical);
    let stem = path.file_stem().unwrap_or_default().to_string_lossy();
    let ext = path
        .extension()
        .map(|e| format!(".{}", e.to_string_lossy()))
        .unwrap_or_default();
    let dir = path
        .parent()
        .map(|p| p.to_string_lossy().into_owned())
        .unwrap_or_default();
    if dir.is_empty() {
        format!("{stem}_{date_str}_{shard_idx:04}{ext}")
    } else {
        format!("{dir}/{stem}_{date_str}_{shard_idx:04}{ext}")
    }
}

async fn open_data_file(path: &str) -> Result<fs::File, Box<dyn std::error::Error + Send + Sync>> {
    ensure_parent_dir(path).await?;

    let data_file = fs::OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .append(true)
        .open(path)
        .await?;

    Ok(data_file)
}

async fn append_line(
    data_file: &mut fs::File,
    line: &Bytes,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    data_file.write_all(line).await?;
    data_file.write_all(b"\n").await?;
    data_file.sync_data().await?;

    Ok(())
}

async fn ensure_parent_dir(path: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if let Some(parent) = PathBuf::from(path).parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent).await?;
        }
    }
    Ok(())
}

impl Drop for AsyncRollbackWriter {
    fn drop(&mut self) {
        self.shutdown_flag.store(true, Ordering::Relaxed);
        tracing::error!("writer dropped");
        // Note: We cannot await the task here as Drop is synchronous.
        // The task should check the shutdown flag and terminate itself.
    }
}

