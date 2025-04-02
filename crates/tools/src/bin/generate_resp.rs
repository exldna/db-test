use clap::Parser;
use db_test_model::temp::RespFilesManager;

fn main() -> anyhow::Result<()> {
    let config = Config::parse();
    for entry in std::fs::read_dir(&config.data_dir)? {
        let data_path = entry?.path();
        if data_path.is_file() {
            RespFilesManager::tar_data_file(&data_path, &config.out_path)?;
        }
    }
    Ok(())
}

#[derive(Clone, Debug, Parser)]
#[command()]
struct Config {
    #[arg()]
    data_dir: Box<std::path::Path>,
    #[arg()]
    out_path: Box<std::path::Path>,
}
