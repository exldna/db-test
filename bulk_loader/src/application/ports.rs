/// A contract for a service that performs Stage 1:
/// sorting raw data into a single, sorted run.
pub trait Sorter {
    fn run(&self) -> anyhow::Result<()>;
}

/// A contract for a service that performs Stage 2:
/// generating SST files from a sorted run and ingesting them.
pub trait SstGenerator {
    fn run(&self) -> anyhow::Result<()>;
}
