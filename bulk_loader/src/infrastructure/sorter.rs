use crate::application::ports::Sorter;
use crate::config::Config;

use dataframe::*;
use datafusion::prelude::*;

pub struct DataFusionSorter {
    config: Config,
}

impl DataFusionSorter {
    pub fn new(config: &Config) -> Self {
        let config = config.clone();
        Self { config }
    }
}

impl Sorter for DataFusionSorter {
    fn run(&self) -> anyhow::Result<()> {
        futures::executor::block_on(self.execute_sort())
    }
}

impl DataFusionSorter {
    async fn execute_sort(&self) -> anyhow::Result<()> {
        let paths = &self.config.paths;
        let num_threads = self.config.bulk_loader.num_threads;

        tracing::debug!(
            "Starting external sort using DataFusion with {} target partitions...",
            num_threads
        );

        tracing::info!("Input directory: {:?}", paths.raw_data_dir);
        tracing::info!("Output directory: {:?}", paths.sorted_runs_dir);

        // Create a session config with our desired level of parallelism.
        let config = SessionConfig::new().with_target_partitions(num_threads);
        let ctx = SessionContext::new_with_config(config);

        let path_str = paths.raw_data_dir.to_string_lossy().to_string();
        let df = ctx
            .read_parquet(&path_str, ParquetReadOptions::default())
            .await?;

        let sorted_df = df
            .select_columns(&["address", "transaction", "time"])?
            .sort(vec![
                col("address").sort(true, true),
                col("time").sort(true, true),
            ])?
            .select_columns(&["address", "transaction"])?;

        // Prepare the output directory and file path.
        std::fs::create_dir_all(&paths.sorted_runs_dir)?;
        let output_file_path = paths.sorted_runs_dir.join("sorted_run.parquet");

        // Clean up any previous run to ensure a fresh start.
        if output_file_path.exists() {
            std::fs::remove_file(&output_file_path)?;
        }

        sorted_df
            .write_parquet(
                &output_file_path.to_string_lossy(),
                DataFrameWriteOptions::new(),
                None,
            )
            .await?;

        tracing::info!("Successfully created sorted run at: {:?}", output_file_path);

        Ok(())
    }
}
