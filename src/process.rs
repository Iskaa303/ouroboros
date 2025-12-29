use eyre::{Context, Result};
use log::{info, trace};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use similar::{TextDiff, udiff::UnifiedDiff};
use std::collections::BTreeSet;
use std::fs;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;
use thiserror::Error;

const CHUNK_SIZE: usize = 128 * 1024 * 1024; // 128MB

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
    pub fn process_all(paths: &BTreeSet<PathBuf>) -> Result<()> {
        let memory_dir = PathBuf::from("memory");
        if !memory_dir.exists() {
            fs::create_dir_all(&memory_dir).context("Failed to create memory directory")?;
        }

        let paths_vec: Vec<_> = paths.iter().collect();
        info!("Starting pipelined processing of {} files", paths_vec.len());

        paths_vec
            .par_iter()
            .try_for_each(|path| Self::pipeline_file(path, &memory_dir))?;

        info!("Finished all processing tasks.");
        Ok(())
    }

    fn pipeline_file(path: &Path, memory_dir: &Path) -> Result<()> {
        let metadata =
            fs::metadata(path).map_err(|_| ProcessError::FileError(path.to_path_buf()))?;
        let current_size = metadata.len();
        let current_mtime = metadata
            .modified()
            .map_err(|_| ProcessError::FileError(path.to_path_buf()))?
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();

        let path_alias = Self::calculate_path_alias(path);
        let target_dir = memory_dir.join(&path_alias);
        let file_basename = path
            .file_name()
            .map(|n| n.to_string_lossy())
            .unwrap_or_default();

        if !target_dir.exists() {
            fs::create_dir_all(&target_dir)
                .map_err(|_| ProcessError::CreateDirError(target_dir.clone()))?;
        }

        let history_path = target_dir.join("history.json");
        let mut history = if history_path.exists() {
            let data = fs::read_to_string(&history_path)
                .map_err(|_| ProcessError::MetadataError(history_path.clone()))?;
            serde_json::from_str(&data).unwrap_or_else(|_| FileHistory {
                alias: path_alias.clone(),
                original_path: path.to_string_lossy().to_string(),
                ..Default::default()
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

        let current_hash = Self::process_chunks_parallel(path)?;

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
            if let Ok(source_content) = fs::read_to_string(path) {
                if let Ok(old_content) = fs::read_to_string(&latest_file_path) {
                    let next_v = history.versions.len() + 1;
                    let diff_name = format!("v{}.diff", next_v);
                    let diff_path = target_dir.join(&diff_name);

                    let text_diff = TextDiff::from_lines(&old_content, &source_content);
                    let diff_text = UnifiedDiff::from_text_diff(&text_diff)
                        .header(file_basename.as_ref(), file_basename.as_ref())
                        .to_string();

                    if !diff_text.is_empty() {
                        fs::write(&diff_path, diff_text)
                            .map_err(|_| ProcessError::FileError(diff_path))?;
                        diff_filename = Some(diff_name);
                    }
                }
            }
        }

        // Finalize: Update latest and record version
        let temp_latest = target_dir.join("latest.tmp");
        fs::copy(path, &temp_latest).map_err(|_| ProcessError::FileError(temp_latest.clone()))?;
        fs::rename(&temp_latest, &latest_file_path)
            .map_err(|_| ProcessError::FileError(latest_file_path))?;

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
        fs::write(&history_path, history_json)
            .map_err(|_| ProcessError::MetadataError(history_path))?;

        info!("[{}] Version v{} stored.", file_basename, next_version);
        Ok(())
    }

    /// Process chunks in parallel and return the overall file hash.
    fn process_chunks_parallel(path: &Path) -> Result<String> {
        let mut file =
            fs::File::open(path).map_err(|_| ProcessError::FileError(path.to_path_buf()))?;
        let file_size = file.metadata()?.len();
        let num_chunks = ((file_size + CHUNK_SIZE as u64 - 1) / CHUNK_SIZE as u64) as usize;
        let file_basename = path
            .file_name()
            .map(|n| n.to_string_lossy())
            .unwrap_or_default();

        if file_size > CHUNK_SIZE as u64 {
            info!("[{}] Processing {} chunks...", file_basename, num_chunks);
        }

        let batch_size = rayon::current_num_threads().max(2);
        let mut file_hasher = Sha256::new();

        for batch_idx in 0..(num_chunks + batch_size - 1) / batch_size {
            let start_chunk = batch_idx * batch_size;
            let end_chunk = std::cmp::min(start_chunk + batch_size, num_chunks);

            let mut batch_data = Vec::with_capacity(batch_size);
            for i in start_chunk..end_chunk {
                let offset = i as u64 * CHUNK_SIZE as u64;
                file.seek(SeekFrom::Start(offset))?;
                let current_chunk_size =
                    std::cmp::min(CHUNK_SIZE as u64, file_size - offset) as usize;
                let mut buffer = vec![0u8; current_chunk_size];
                file.read_exact(&mut buffer)?;
                batch_data.push(buffer);
            }

            let hashes: Vec<String> = batch_data
                .into_par_iter()
                .enumerate()
                .map(|(idx, data)| {
                    let chunk_idx = start_chunk + idx;
                    let hash = Self::calculate_data_hash(&data);
                    if file_size > 500 * 1024 * 1024 {
                        info!(
                            "[{}] Finished chunk {}/{}",
                            file_basename,
                            chunk_idx + 1,
                            num_chunks
                        );
                    }
                    hash
                })
                .collect();

            for hash in hashes {
                file_hasher.update(hash.as_bytes());
            }

            if file_size > 500 * 1024 * 1024 {
                let progress = (end_chunk as f64 / num_chunks as f64) * 100.0;
                info!("[{}] Hashing progress: {:.1}%", file_basename, progress);
            }
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
