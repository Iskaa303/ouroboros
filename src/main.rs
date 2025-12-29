mod process;
mod storage;

use eyre::Result;
use log::info;
use process::Processor;
use storage::FileStorage;

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    env_logger::init();

    info!("Starting Parallel Versioned Storage...");

    let mut storage = FileStorage::new();
    storage.add("./tests/test2.txt").await;
    storage.add("./tests").await;
    storage
        .add(vec![
            "./src/main.rs",
            "./src/storage.rs",
            "./src/process.rs",
        ])
        .await;

    info!("Collected {} unique files", storage.len());

    Processor::process_all(storage.paths())?;

    Ok(())
}
