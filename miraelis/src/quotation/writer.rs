use barter_data::{
    event::{DataKind, MarketEvent},
};
use chrono::Utc;
use crossbeam_channel::{self, TryRecvError};
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
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

/// 每一个逻辑文件路径（如 `{dir}/trades.jsonl`）对应的分片状态。
/// 物理路径格式：`{dir}/{stem}_{date}_{idx:04}{ext}`
struct ShardState {
    /// 当前物理路径
    physical_path: String,
    /// 当前活跃日期字符串 ("YYYY-MM-DD")
    date_str: String,
    /// 当前分片序号（从 1 开始）
    shard_idx: u32,
    /// 当前分片已写入字节数
    current_bytes: u64,
    data_file: fs::File,
}

struct WriteRequest {
    /// 逻辑路径，不含日期/序号后缀
    path: String,
    line: Vec<u8>,
}

#[derive(Clone)]
pub struct AsyncRollbackWriter {
    tx: crossbeam_channel::Sender<MarketEvent<String, DataKind>>,
    output_dir: String,
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

        let task = tokio::spawn(async move {
            let output_dir = output_dir_clone;
            // key = logical path
            let mut shards: HashMap<String, ShardState> = HashMap::new();

            loop {
                let event = match rx.try_recv() {
                    Ok(event) => event,
                    Err(TryRecvError::Empty) => {
                        sleep(Duration::from_millis(2)).await;
                        continue;
                    }
                    Err(TryRecvError::Disconnected) => break,
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
                        let req = WriteRequest {
                            path: format!("{}/trades.jsonl", output_dir),
                            line: serde_json::to_vec(&payload).unwrap_or_default(),
                        };
                        if let Err(e) = write_sharded(&req, &mut shards, max_shard_bytes).await {
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
                                line: serde_json::to_vec(&payload).unwrap_or_default(),
                            };
                            if let Err(e) = write_sharded(&req, &mut shards, max_shard_bytes).await
                            {
                                warn!("Failed to write candle event: {:?}", e);
                            }
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
                            line: serde_json::to_vec(&payload).unwrap_or_default(),
                        };
                        if let Err(e) = write_sharded(&req, &mut shards, max_shard_bytes).await {
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
                                line: serde_json::to_vec(&payload).unwrap_or_default(),
                            };
                            if let Err(e) = write_sharded(&req, &mut shards, max_shard_bytes).await
                            {
                                warn!("Failed to write candle event: {:?}", e);
                            }
                        }
                    }
                    _ => {}
                }
            }
        });

        (
            Self {
                tx,
                output_dir: output_dir.to_string(),
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
                    ShardState {
                        physical_path: physical,
                        date_str: new_date,
                        shard_idx: new_idx,
                        current_bytes,
                        data_file,
                    },
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
    match append_line(&mut shard.data_file, &req.line).await {
        Ok(()) => {
            shard.current_bytes += req.line.len() as u64 + 1; // +1 for newline
        }
        Err(error) => {
            warn!(path = %shard.physical_path, ?error, "append line failed");
        }
    }

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
    line: &[u8],
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

