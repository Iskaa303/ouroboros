mod process;
mod storage;

use eyre::Result;
use log::{error, info, warn};
use process::Processor;
use storage::FileStorage;

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();

    env_logger::Builder::from_default_env()
        .format(|buf, record| {
            use std::io::Write;
            let level = record.level();
            let color = match level {
                log::Level::Error => "\x1b[31;1m",
                log::Level::Warn => "\x1b[33m",
                log::Level::Info => "\x1b[32m",
                log::Level::Debug => "\x1b[34m",
                log::Level::Trace => "\x1b[35m",
            };
            let reset = "\x1b[0m";

            writeln!(
                buf,
                "[{}] {}{:5}{} {}] {}",
                chrono::Local::now().format("%Y-%m-%dT%H:%M:%S"),
                color,
                level,
                reset,
                record.target(),
                record.args()
            )
        })
        .init();

    info!("Starting Parallel Versioned Storage...");

    let mut storage = FileStorage::new();

    if let Ok(content) = tokio::fs::read_to_string("ingest.txt").await {
        for line in content.lines() {
            let path = line.trim();
            if !path.is_empty() && !path.starts_with('#') {
                storage.add(path).await;
            }
        }
    } else {
        warn!("ingest.txt not found or unreadable, skipping initial ingestion");
    }

    info!("Collected {} unique files", storage.len());

    if let Err(e) = Processor::process_all(storage.paths()).await {
        error!("Fatal error during processing: {:?}", e);
        std::process::exit(1);
    }

    Ok(())
}
