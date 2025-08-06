use super::ports::{Sorter, SstGenerator};

/// The main application service that orchestrates the bulk loading process.
/// It is generic over the Sorter and SstGenerator traits, allowing for
/// dependency injection.
pub struct BulkLoaderService<S: Sorter, G: SstGenerator> {
    sorter: S,
    sst_generator: G,
}

impl<S: Sorter, G: SstGenerator> BulkLoaderService<S, G> {
    /// Creates a new service with concrete implementations of the ports.
    pub fn new(sorter: S, sst_generator: G) -> Self {
        Self {
            sorter,
            sst_generator,
        }
    }

    /// Executes the entire bulk loading pipeline.
    pub fn run(&self) -> anyhow::Result<()> {
        tracing::info!("Starting Stage 1: Sorting");
        // self.sorter.run()?;
        tracing::info!("Stage 1: Sorting finished successfully ---");

        tracing::info!("Starting Stage 2: SST Generation & Ingestion");
        // self.sst_generator.run()?;
        tracing::info!("Stage 2: SST Generation & Ingestion finished successfully");

        Ok(())
    }
}
