pub mod bulk_data;
pub mod temp;

fn out_dir_path() -> Box<std::path::Path> {
    let out_dir = env!("OUT_DIR");
    std::path::Path::new(out_dir).into()
}

pub fn generate_data(qualties: impl Iterator<Item = u64>) -> anyhow::Result<()> {
    let out_dir = out_dir_path();
    for quality in qualties {
        let file_name = format!("data_{}.csv", quality);
        let file_path = out_dir.join(file_name.as_str());
        if let Ok(file) = std::fs::File::create_new(file_path) {
            write_data_file(&file, quality)?;
        }
    }
    Ok(())
}

fn write_data_file(file: &std::fs::File, quality: u64) -> anyhow::Result<()> {
    let mut csv_file = csv::Writer::from_writer(file);
    let transactions = bulk_data::BulkDataGenerator::new();
    for transaction in transactions.take(quality as usize) {
        transaction.serialize_csv(&mut csv_file)?;
    }
    Ok(())
}

pub fn list_data_files() -> anyhow::Result<impl Iterator<Item = std::path::PathBuf>> {
    // IMPLEMENTATION NOTES:
    // We need to iterate over all `out_dir` entiries to catch any io errors.
    // Otherwise, we have to unwrap this values, which might be unexpected.
    
    let out_dir = out_dir_path();

    let mut files = vec![];
    for entry in std::fs::read_dir(out_dir)? {
        let path = entry?.path();
        if path.is_file() {
            files.push(path);
        }
    }

    files.sort();

    Ok(files.into_iter())
}
