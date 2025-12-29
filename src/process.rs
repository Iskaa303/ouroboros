use eyre::{Context, Result};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use log::{debug, error, info, trace, warn};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use similar::{TextDiff, udiff::UnifiedDiff};
use std::collections::BTreeSet;
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;
use thiserror::Error;

const CHUNK_SIZE: usize = 8 * 1024 * 1024; // 8MB

#[derive(Error, Debug)]
pub enum ProcessError {
    #[error("failed to create directory: {0}")]
    CreateDirError(PathBuf),
    #[error("failed to handle file: {0}")]
    FileError(PathBuf),
    #[error("failed to write metadata: {0}")]
    MetadataError(PathBuf),
}

#[derive(Serialize, Deserialize, Default)]
struct FileHistory {
    versions: Vec<FileVersion>,
    alias: String,
    original_path: String,
}

#[derive(Serialize, Deserialize, Clone)]
struct FileVersion {
    version: u32,
    hash: String,
    size: u64,
    mtime_ns: u128,
    processed_at: String,
    diff_file: Option<String>,
}

pub struct Processor;

impl Processor {
    pub async fn process_all(paths: &BTreeSet<PathBuf>) -> Result<()> {
        let memory_dir = PathBuf::from("memory");
        if !memory_dir.exists() {
            fs::create_dir_all(&memory_dir).context("Failed to create memory directory")?;
        }

        let paths_vec: Vec<_> = paths.iter().cloned().collect();
        info!(
            "Starting parallel async processing of {} files",
            paths_vec.len()
        );

        let mut handles = Vec::new();
        let semaphore = std::sync::Arc::new(tokio::sync::Semaphore::new(512)); // Global concurrency limit
        let multi = std::sync::Arc::new(MultiProgress::new());

        for path in paths_vec {
            let memory_dir = memory_dir.clone();
            let semaphore = semaphore.clone();
            let multi = multi.clone();
            handles.push(tokio::spawn(async move {
                Self::pipeline_file(path, memory_dir, semaphore, multi).await
            }));
        }

        for handle in handles {
            if let Err(e) = handle.await.wrap_err("Task panicked")? {
                error!("A processing task failed: {:?}", e);
                return Err(e);
            }
        }

        info!("Finished all processing tasks.");
        Ok(())
    }

    async fn pipeline_file(
        path: PathBuf,
        memory_dir: PathBuf,
        semaphore: std::sync::Arc<tokio::sync::Semaphore>,
        multi: std::sync::Arc<MultiProgress>,
    ) -> Result<()> {
        let path_ref = &path;
        let metadata = tokio::fs::metadata(path_ref).await.map_err(|e| {
            error!("Failed to get metadata for {}: {}", path_ref.display(), e);
            ProcessError::FileError(path_ref.to_path_buf())
        })?;
        let current_size = metadata.len();
        let current_mtime = metadata
            .modified()
            .map_err(|_| ProcessError::FileError(path.to_path_buf()))?
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();

        let path_alias = Self::calculate_path_alias(&path);
        let target_dir = memory_dir.join(&path_alias);
        let file_basename = path
            .file_name()
            .map(|n| n.to_string_lossy())
            .unwrap_or_default();

        if !target_dir.exists() {
            tokio::fs::create_dir_all(&target_dir).await.map_err(|e| {
                error!(
                    "Failed to create target dir {}: {}",
                    target_dir.display(),
                    e
                );
                ProcessError::CreateDirError(target_dir.clone())
            })?;
        }

        let history_path = target_dir.join("history.json");
        let mut history = if history_path.exists() {
            let data = tokio::fs::read_to_string(&history_path)
                .await
                .map_err(|e| {
                    error!(
                        "Failed to read history file {}: {}",
                        history_path.display(),
                        e
                    );
                    ProcessError::MetadataError(history_path.clone())
                })?;
            serde_json::from_str(&data).unwrap_or_else(|_| {
                warn!(
                    "Failed to parse history.json for {}. Recreating.",
                    path.display()
                );
                FileHistory {
                    alias: path_alias.clone(),
                    original_path: path.to_string_lossy().to_string(),
                    ..Default::default()
                }
            })
        } else {
            FileHistory {
                alias: path_alias.clone(),
                original_path: path.to_string_lossy().to_string(),
                ..Default::default()
            }
        };

        if let Some(latest) = history.versions.last() {
            if latest.size == current_size && latest.mtime_ns == current_mtime {
                trace!("[{}] Skipping unchanged file (metadata).", file_basename);
                return Ok(());
            }
        }

        let current_hash = Self::process_chunks_parallel(&path, semaphore, multi).await?;

        // Deep change detection
        if let Some(latest) = history.versions.last() {
            if latest.hash == current_hash {
                trace!("[{}] Skipping unchanged file (content).", file_basename);
                // Update metadata if hash matched but mtime didn't
                return Ok(());
            }
        }

        // Diff and Storage Stage
        let latest_file_path = target_dir.join("latest");
        let mut diff_filename = None;

        if latest_file_path.exists() && current_size < CHUNK_SIZE as u64 {
            if let Ok(source_content) = tokio::fs::read_to_string(&path).await {
                if let Ok(old_content) = tokio::fs::read_to_string(&latest_file_path).await {
                    let next_v = history.versions.len() + 1;
                    let diff_name = format!("v{}.diff", next_v);
                    let diff_path = target_dir.join(&diff_name);

                    let text_diff = TextDiff::from_lines(&old_content, &source_content);
                    let diff_text = UnifiedDiff::from_text_diff(&text_diff)
                        .header(file_basename.as_ref(), file_basename.as_ref())
                        .to_string();

                    if !diff_text.is_empty() {
                        tokio::fs::write(&diff_path, diff_text).await.map_err(|e| {
                            error!("Failed to write diff file {}: {}", diff_path.display(), e);
                            ProcessError::FileError(diff_path)
                        })?;
                        diff_filename = Some(diff_name);
                    }
                } else {
                    debug!(
                        "Could not read old content from {} for diffing.",
                        latest_file_path.display()
                    );
                }
            } else {
                debug!(
                    "Could not read source content from {} for diffing.",
                    path.display()
                );
            }
        }

        // Finalize: Update latest and record version
        let temp_latest = target_dir.join("latest.tmp");
        tokio::fs::copy(&path, &temp_latest).await.map_err(|e| {
            error!(
                "Failed to copy {} to {}: {}",
                path.display(),
                temp_latest.display(),
                e
            );
            ProcessError::FileError(temp_latest.clone())
        })?;
        tokio::fs::rename(&temp_latest, &latest_file_path)
            .await
            .map_err(|e| {
                error!(
                    "Failed to rename {} to {}: {}",
                    temp_latest.display(),
                    latest_file_path.display(),
                    e
                );
                ProcessError::FileError(latest_file_path)
            })?;

        let next_version = history.versions.len() as u32 + 1;
        history.versions.push(FileVersion {
            version: next_version,
            hash: current_hash,
            size: current_size,
            mtime_ns: current_mtime,
            processed_at: chrono::Local::now().to_rfc3339(),
            diff_file: diff_filename,
        });

        let history_json =
            serde_json::to_string_pretty(&history).wrap_err("Failed to serialize history")?;
        tokio::fs::write(&history_path, history_json)
            .await
            .map_err(|e| {
                error!(
                    "Failed to write history file {}: {}",
                    history_path.display(),
                    e
                );
                ProcessError::MetadataError(history_path)
            })?;

        info!("[{}] Version v{} stored.", file_basename, next_version);
        Ok(())
    }

    async fn process_chunks_parallel(
        path: &Path,
        _semaphore: std::sync::Arc<tokio::sync::Semaphore>, // Kept for signature, but Rayon handles parallel CPU
        multi: std::sync::Arc<MultiProgress>,
    ) -> Result<String> {
        let file_size = fs::metadata(path)
            .map_err(|_| ProcessError::FileError(path.to_path_buf()))?
            .len();
        let num_chunks = ((file_size + CHUNK_SIZE as u64 - 1) / CHUNK_SIZE as u64) as usize;
        let file_basename = path
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default();

        let pb = multi.add(ProgressBar::new(num_chunks as u64));
        pb.set_style(ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({percent}%) {msg}")?
            .progress_chars("#>-"));
        pb.set_message(file_basename.clone());

        let (tx, rx) =
            std::sync::mpsc::sync_channel::<Result<(usize, Vec<u8>), std::io::Error>>(128); // Deeper buffer
        let file_path = path.to_path_buf();

        // I/O Producer Thread: Fast sequential reads
        std::thread::spawn(move || {
            let mut f = match fs::File::open(&file_path) {
                Ok(f) => f,
                Err(e) => {
                    let _ = tx.send(Err(e)); // Send the error to the consumer
                    return;
                }
            };

            for i in 0..num_chunks {
                let offset = i as u64 * CHUNK_SIZE as u64;
                let current_chunk_size =
                    std::cmp::min(CHUNK_SIZE as u64, file_size - offset) as usize;

                let mut buffer = vec![0u8; current_chunk_size];
                use std::io::Seek;
                if let Err(e) = f.seek(std::io::SeekFrom::Start(offset)) {
                    let _ = tx.send(Err(e));
                    break;
                }
                if let Err(e) = f.read_exact(&mut buffer) {
                    let _ = tx.send(Err(e));
                    break;
                }
                if tx.send(Ok((i, buffer))).is_err() {
                    // Receiver disconnected, likely due to an error or early termination
                    break;
                }
            }
        });

        let pb_inner = pb.clone();
        let results = tokio::task::spawn_blocking(move || {
            rx.into_iter()
                .par_bridge()
                .map(|item| {
                    let (idx, data) = item?;
                    let hash = Self::calculate_data_hash(&data);
                    pb_inner.inc(1);
                    Ok::<(usize, String), std::io::Error>((idx, hash))
                })
                .collect::<Result<Vec<(usize, String)>, std::io::Error>>()
        })
        .await
        .wrap_err("Parallel hashing task panicked")??;

        if results.len() != num_chunks {
            error!(
                "[{}] Chunk count mismatch: expected {}, got {}",
                file_basename,
                num_chunks,
                results.len()
            );
            return Err(eyre::eyre!("Chunk count mismatch for {}", file_basename));
        }

        pb.finish_with_message(format!("{} [DONE]", file_basename));

        info!("[{}] Hashing complete. Ordering results...", file_basename);

        let mut sorted_results = results;
        sorted_results.sort_by_key(|(idx, _)| *idx);
        let mut file_hasher = Sha256::new();
        for (_, hash) in sorted_results {
            file_hasher.update(hash.as_bytes());
        }

        Ok(format!("{:x}", file_hasher.finalize()))
    }

    fn calculate_data_hash(data: &[u8]) -> String {
        let mut hasher = Sha256::new();
        hasher.update(data);
        format!("{:x}", hasher.finalize())
    }

    fn calculate_path_alias(path: &Path) -> String {
        let mut hasher = Sha256::new();
        hasher.update(path.to_string_lossy().as_bytes());
        let hash = format!("{:x}", hasher.finalize());
        let filename = path
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_else(|| "unknown".to_string());
        format!("{}_{}", &hash[..12], filename)
    }
}
