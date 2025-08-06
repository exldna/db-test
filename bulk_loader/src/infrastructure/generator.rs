use anyhow::{Context, Result};
use arrow::{
    array::StringArray,
    record_batch::{RecordBatch, RecordBatchReader},
};
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};
use rocksdb::{DB, Options, SstFileWriter};

use std::{fs, fs::File, path::Path};

use crate::{application::ports::SstGenerator, config::AppConfig};

// --- Public Struct ---

/// An adapter that implements the `SstGenerator` port.
///
/// This implementation reads a single, large, sorted Parquet file,
/// transforms the data into RocksDB-compatible key-value pairs via a custom
/// iterator, and writes them into a single SST file. Finally, it ingests
/// this file into the RocksDB database.
pub struct RocksSstGenerator {
    config: AppConfig,
}

impl RocksSstGenerator {
    /// Creates a new `RocksSstGenerator` with the application config.
    pub fn new(config: &AppConfig) -> Self {
        Self {
            config: config.clone(),
        }
    }
}

// --- Port Implementation ---

impl SstGenerator for RocksSstGenerator {
    /// Orchestrates the entire Stage 2 process: SST generation and ingestion.
    fn run(&self) -> Result<()> {
        let paths = &self.config.paths;
        let sst_output_path = paths.sst_files_dir.join("data.sst");
        let sorted_run_path = paths.sorted_runs_dir.join("sorted_run.parquet");

        fs::create_dir_all(&paths.sst_files_dir)?;
        fs::create_dir_all(&paths.rocks_db_dir)?;

        let mut db_opts = Options::default();
        db_opts.create_if_missing(true);

        // Execute the two main steps
        Self::generate_sst_file(self, &sorted_run_path, &sst_output_path, &db_opts)?;
        Self::ingest_sst_file(self, &sst_output_path, &db_opts)?;

        Ok(())
    }
}

// --- Core Logic ---

impl RocksSstGenerator {
    /// Generates a single SST file from a sorted Parquet "run" file.
    ///
    /// This function acts as a high-level coordinator that sets up the file reader
    /// and the SST writer, then passes them to the iterator for processing.
    fn generate_sst_file(
        &self,
        sorted_run_path: &Path,
        sst_output_path: &Path,
        db_opts: &Options,
    ) -> Result<()> {
        tracing::info!("Generating SST file at {:?}...", sst_output_path);

        let mut writer = SstFileWriter::create(db_opts);
        writer.open(sst_output_path)?;

        let file = File::open(sorted_run_path)
            .with_context(|| format!("Failed to open sorted run file: {:?}", sorted_run_path))?;

        let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;

        for kv_result in KeyValueIterator::new(reader) {
            let (key, value) = kv_result?;
            writer.put(key, &value)?;
        }

        writer.finish()?;
        tracing::info!("SST file generation complete.");
        Ok(())
    }

    /// Ingests the newly created SST file into the RocksDB database.
    fn ingest_sst_file(&self, sst_path: &Path, db_opts: &Options) -> Result<()> {
        tracing::info!("Ingesting SST file into RocksDB...");

        let db = DB::open(db_opts, &self.config.paths.rocks_db_dir)?;
        db.ingest_external_file(&[sst_path])?;

        tracing::info!("Ingestion complete and temporary SST file removed.");
        Ok(())
    }
}

// --- Custom Iterator Logic ---

/// A generic iterator that wraps any `RecordBatchReader` and yields RocksDB key-value pairs.
struct KeyValueIterator<R: RecordBatchReader + Sized> {
    reader: R,
    current_batch_iter: std::vec::IntoIter<(Vec<u8>, Vec<u8>)>,
    current_address: String,
    sequence_number: u64,
}

impl<R: RecordBatchReader + Sized> KeyValueIterator<R> {
    /// Creates a new iterator from any type that implements `RecordBatchReader`.
    pub fn new(reader: R) -> Self {
        Self {
            reader,
            current_batch_iter: Vec::new().into_iter(),
            current_address: String::new(),
            sequence_number: 0,
        }
    }

    /// Processes a `RecordBatch`, converting its rows into key-value pairs.
    fn process_batch(&mut self, batch: RecordBatch) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let addresses = batch
            .column_by_name("address")
            .and_then(|col| col.as_any().downcast_ref::<StringArray>())
            .context("Failed to downcast 'address' column")?;

        let tx_hashes = batch
            .column_by_name("transaction")
            .and_then(|col| col.as_any().downcast_ref::<StringArray>())
            .context("Failed to downcast 'transaction' column")?;

        let mut results = Vec::with_capacity(batch.num_rows());

        for (address, tx_hash) in addresses.iter().zip(tx_hashes.iter()) {
            let (address, tx_hash) = (
                address.context("Found null in 'address' column")?,
                tx_hash.context("Found null in 'transaction' column")?,
            );

            if address != self.current_address {
                self.current_address = address.to_string();
                self.sequence_number = 0;
            }

            let key = {
                let mut key = Vec::with_capacity(self.current_address.len() + 8);
                key.extend_from_slice(self.current_address.as_bytes());
                key.extend_from_slice(&self.sequence_number.to_be_bytes());
                key
            };

            results.push((key, tx_hash.as_bytes().to_vec()));
            self.sequence_number += 1;
        }

        Ok(results)
    }
}

impl<R: RecordBatchReader + Sized> Iterator for KeyValueIterator<R> {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        // If the current batch iterator has items, return the next one.
        if let Some(kv) = self.current_batch_iter.next() {
            return Some(Ok(kv));
        }

        // If the batch is exhausted, try to load and process the next one from the reader.
        match self.reader.next() {
            Some(Ok(batch)) => {
                // We have a new batch, process it into a vector of key-values.
                match self.process_batch(batch) {
                    Ok(kvs) => self.current_batch_iter = kvs.into_iter(),
                    Err(e) => return Some(Err(e)),
                }
            }
            Some(Err(e)) => return Some(Err(e.into())), // Arrow read error
            None => return None,                        // End of data
        }

        // Return the first item of the newly populated batch iterator.
        self.current_batch_iter.next().map(Ok)
    }
}
