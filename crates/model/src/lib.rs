pub mod bulk_data;
pub mod temp;

pub fn out_dir_path() -> Box<std::path::Path> {
    let out_dir = env!("OUT_DIR");
    std::path::Path::new(out_dir).into()
}

pub fn generate_data(
    out_dir: &std::path::Path, 
    qualties: impl Iterator<Item = usize>
) -> anyhow::Result<()> {
    for quality in qualties {
        let file_name = format!("data_{}.csv", quality);
        let file_path = out_dir.join(file_name.as_str());
        if let Ok(file) = std::fs::File::create_new(file_path) {
            write_data_file(&file, quality)?;
        }
    }
    Ok(())
}

fn write_data_file(file: &std::fs::File, quality: usize) -> anyhow::Result<()> {
    let mut csv_file = csv::Writer::from_writer(file);
    let transactions = bulk_data::BulkDataGenerator::new();
    for transaction in transactions.take(quality) {
        transaction.serialize_csv(&mut csv_file)?;
    }
    Ok(())
}

pub fn list_data_files() -> anyhow::Result<impl Iterator<Item = (u64, std::path::PathBuf)>> {
    // IMPLEMENTATION NOTES:
    // We need to iterate over all `out_dir` entiries to catch any io errors.
    // Otherwise, we have to unwrap this values, which might be unexpected.

    let out_dir = out_dir_path();

    let mut files = vec![];
    for entry in std::fs::read_dir(out_dir)? {
        let path = entry?.path();
        if path.is_file() {
            let quality = get_quality(&path)?;
            files.push((quality, path));
        }
    }

    files.sort();

    Ok(files.into_iter())
}

fn get_quality(path: &std::path::Path) -> anyhow::Result<u64> {
    let Some(stem) = path.file_stem() else {
        anyhow::bail!("invalid data file path: {}", path.display());
    };
    let Some(stem) = stem.to_str() else {
        anyhow::bail!("non ascii character in data file path");
    };
    let quality = stem[5..].parse::<u64>()?;
    Ok(quality)
}
