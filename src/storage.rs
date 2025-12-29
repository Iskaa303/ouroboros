use log::{debug, trace, warn};
use std::collections::BTreeSet;
use std::path::PathBuf;
use tokio::fs as tfs;

#[derive(Default, Debug)]
pub struct FileStorage {
    paths: BTreeSet<PathBuf>,
}

impl FileStorage {
    pub fn new() -> Self {
        trace!("Initializing new FileStorage");
        Self::default()
    }

    pub async fn add(&mut self, source: impl Into<AddingSource>) -> &mut Self {
        match source.into() {
            AddingSource::Single(path) => {
                let path_buf = PathBuf::from(path);
                self.add_recursive(path_buf).await;
            }
            AddingSource::Many(paths) => {
                for path in paths {
                    let path_buf = PathBuf::from(path);
                    self.add_recursive(path_buf).await;
                }
            }
        }
        self
    }

    async fn add_recursive(&mut self, path: PathBuf) {
        if !path.exists() {
            warn!("Path does not exist: {}", path.display());
            return;
        }

        if tfs::metadata(&path)
            .await
            .map(|m| m.is_dir())
            .unwrap_or(false)
        {
            if let Ok(mut entries) = tfs::read_dir(&path).await {
                while let Ok(Some(entry)) = entries.next_entry().await {
                    Box::pin(self.add_recursive(entry.path())).await;
                }
            }
        } else {
            match tfs::canonicalize(&path).await {
                Ok(canonical) => {
                    debug!("Adding file: {}", canonical.display());
                    self.paths.insert(canonical);
                }
                Err(_) => {
                    self.paths.insert(path);
                }
            }
        }
    }

    pub fn len(&self) -> usize {
        self.paths.len()
    }

    pub fn paths(&self) -> &BTreeSet<PathBuf> {
        &self.paths
    }
}

pub enum AddingSource {
    Single(String),
    Many(Vec<String>),
}

impl From<&str> for AddingSource {
    fn from(s: &str) -> Self {
        Self::Single(s.to_string())
    }
}

impl From<String> for AddingSource {
    fn from(s: String) -> Self {
        Self::Single(s)
    }
}

impl From<Vec<String>> for AddingSource {
    fn from(v: Vec<String>) -> Self {
        Self::Many(v)
    }
}

impl From<Vec<&str>> for AddingSource {
    fn from(v: Vec<&str>) -> Self {
        Self::Many(v.into_iter().map(|s| s.to_string()).collect())
    }
}
