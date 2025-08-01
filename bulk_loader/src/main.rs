mod application;
mod config;
mod infrastructure;

use anyhow::Result;
use config::get_config;
use tracing_subscriber::{EnvFilter, FmtSubscriber};

use application::service::BulkLoaderService;
use infrastructure::{generator::RocksSstGenerator, sorter::DataFusionSorter};

fn setup_tracing(level: &str) -> Result<()> {
    let filter = EnvFilter::builder()
        .with_default_directive(level.parse()?)
        .from_env_lossy();

    FmtSubscriber::builder()
        .with_env_filter(filter)
        .with_target(false)
        .compact()
        .init();

    Ok(())
}

fn main() -> Result<()> {
    let config = get_config()?;
    setup_tracing(&config.logging.level)?;
    tracing::info!("Configuration loaded successfully");

    tracing::info!("Starting bulk_loader...");
    tracing::debug!(?config, "Full application configuration");

    let sorter = DataFusionSorter::new(&config);
    let sst_generator = RocksSstGenerator::new(&config);

    let service = BulkLoaderService::new(sorter, sst_generator);

    if let Err(e) = service.run() {
        tracing::error!("Application finished with an error: {:?}", e);
        std::process::exit(1);
    }

    tracing::info!("Bulk loading completed successfully!");
    Ok(())
}
