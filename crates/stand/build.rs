fn main() -> anyhow::Result<()> {
    let qualities = (500..10_000).step_by(1000);
    db_test_model::generate_data(qualities)?;
    Ok(())
}

// fn generate_inser_bulk_data(out_dir: &std::path::Path) -> BuildResult {
//     std::fs::create_dir_all(out_dir)?;
//     for i in (500..10_000).step_by(1_000) {
//         let out_file_name = format!("data_{i}.csv");
//         let out_file_path = out_dir.join(out_file_name.as_str());
//         let file = std::fs::File::create_new(out_file_path.clone());
//         let Ok(mut file) = file else {
//             println!("cargo::warning=data file {i} already exists");
//             continue; // do not regenerate existing files
//         };
//         println!("cargo::warning=try regenerate data file {i}");
//         println!("cargo::rerun-if-changed={}", out_file_path.display());
//         let transactions = gen_transactions(i);
//         insert_bulk_fill_data_file(transactions, &mut file)?;
//     }
//     Ok(())
// }

// fn insert_bulk_fill_data_file(
//     transactions: impl Iterator<Item = Transaction>,
//     file: &mut std::fs::File,
// ) -> BuildResult {
//     let mut writer = csv::Writer::from_writer(file);
//     for transaction in transactions {
//         writer.write_record(&[
//             transaction.0.as_str(),
//             transaction.1.to_string().as_str(),
//             transaction.2.as_str(),
//         ])?;
//     }
//     Ok(())
// }
