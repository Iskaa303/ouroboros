use eyre::Result;
use ouroboros::ingest;
use std::path::PathBuf;

#[tokio::main]
async fn main() -> Result<()> {
    let test_path = PathBuf::from("test.txt");

    ingest::process(test_path.clone()).await?;
    println!("Processed file: {:?}", test_path);

    Ok(())
}
