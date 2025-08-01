use project_root::get_project_root;

use clap::Parser;
use figment::{
    Figment,
    providers::{Format, Toml},
};

use std::path::PathBuf;

/// A single, unified struct holding all application settings.
/// It is deserialized from the TOML file.
#[derive(serde::Deserialize, Debug, Clone)]
#[allow(dead_code)]
pub struct Config {
    pub logging: LoggingConfig,
    pub paths: PathsConfig,
    pub bulk_loader: BulkLoaderConfig,
}

#[derive(serde::Deserialize, Debug, Clone)]
#[allow(dead_code)]
pub struct LoggingConfig {
    pub level: String,
}

#[derive(serde::Deserialize, Debug, Clone)]
#[allow(dead_code)]
pub struct PathsConfig {
    pub raw_data_dir: PathBuf,
}

#[derive(serde::Deserialize, Debug, Clone)]
#[allow(dead_code)]
pub struct BulkLoaderConfig {
    pub num_threads: usize,
}

/// Parses command-line arguments using the clap derive macro.
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
#[allow(dead_code)]
struct Cli {
    #[arg(short, long)]
    pub num_threads: Option<usize>,
}

/// Loads configuration from the TOML file and merges it with CLI arguments.
pub fn get_config() -> anyhow::Result<Config> {
    let cli = Cli::parse();

    let config_path = get_project_root()?.join("config/settings.toml");
    let mut figment = Figment::new().merge(Toml::file(config_path));

    if let Some(cli_threads) = cli.num_threads {
        figment = figment.merge(("bulk_loader.num_threads", cli_threads));
    }

    let mut config: Config = figment.extract()?;

    if config.bulk_loader.num_threads == 0 {
        let num_threads = std::thread::available_parallelism()?.get();
        config.bulk_loader.num_threads = num_threads;
    }

    Ok(config)
}
