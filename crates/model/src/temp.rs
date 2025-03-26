use std::io::Write;

pub struct RespFilesManager;

impl RespFilesManager {
    fn redis_insert_command(record: &csv::StringRecord) -> [&str; 4] {
        ["ZADD", &record[0], &record[1], &record[2]]
    }

    pub fn tar_data_file(
        csv_file_path: &std::path::Path,
        dst_file_path: &std::path::Path,
    ) -> anyhow::Result<std::path::PathBuf> {
        let tar_file_stem = csv_file_path.file_stem().ok_or(anyhow::anyhow!(
            "invalid file path: {}",
            csv_file_path.display()
        ))?;
        // `..some path/resp/_csv_file_stem_.tar`
        let tar_file_path = csv_file_path
            .with_file_name("resp")
            .join(tar_file_stem)
            .with_extension("tar");
        if !tar_file_path.exists() {
            std::fs::create_dir_all(tar_file_path.parent().unwrap())?;
            Self::try_cache_tar_file(csv_file_path, &tar_file_path, dst_file_path)?;
        }
        Ok(tar_file_path)
    }

    fn try_cache_tar_file(
        csv_file_path: &std::path::Path,
        tar_file_path: &std::path::Path,
        dst_file_path: &std::path::Path,
    ) -> anyhow::Result<()> {
        let mut csv_reader = csv::Reader::from_path(csv_file_path)?;
        let mut tar_file = tar::Builder::new(std::fs::File::create(&tar_file_path)?);
        let mut tar_header = tar::Header::new_gnu();
        let dst_file_name = dst_file_path.file_name().unwrap();
        let mut writer = tar_file.append_writer(&mut tar_header, dst_file_name)?;
        for result in csv_reader.records() {
            let record = result?;
            let command = Self::redis_insert_command(&record);
            let resp_item = resp::encode_slice(&command);
            writer.write_all(&resp_item)?;
        }
        Ok(())
    }
}
