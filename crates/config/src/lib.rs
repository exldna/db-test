mod serde_duration;

// pub fn get_config_path() -> std::path::PathBuf {
//     let config_path = env!("CONFIG_FILE_PATH");
//     std::path::PathBuf::from(config_path)
// }

#[derive(Clone, Debug, serde::Deserialize)]
pub struct Config {
    pub data: DataConfig,
    #[serde(rename = "bench")]
    pub benches: Benches,
}

impl Config {
    // pub fn try_read() -> anyhow::Result<Self> {
    //     Config::from_file(env!("CONFIG_FILE"))
    // }

    pub fn from_file(config_file: &str) -> anyhow::Result<Self> {
        let settings = config::Config::builder()
            .add_source(config::File::with_name(config_file))
            .build()?;

        let config = settings.try_deserialize()?;
        Ok(config)
    }
}

#[derive(Clone, Debug, serde::Deserialize)]
pub struct DataConfig {
    pub range: std::ops::RangeInclusive<usize>,
    pub step: usize,
}

impl IntoIterator for DataConfig {
    type IntoIter = std::iter::StepBy<std::ops::RangeInclusive<usize>>;
    type Item = usize;

    fn into_iter(self) -> Self::IntoIter {
        self.range.step_by(self.step)
    }
}

#[derive(Clone, Debug, serde::Deserialize)]
pub struct Benches {
    pub insert_bulk: BenchConfig,
}

#[derive(Clone, Debug, serde::Deserialize)]
pub struct BenchConfig {
    #[serde(rename = "sample-size")]
    pub sample_size: u32,
    #[serde(
        rename = "warm-up-time",
        deserialize_with = "serde_duration::deserialize"
    )]
    pub warm_up_time: std::time::Duration,
}

// #[cfg(test)]
// mod tests {
//     use crate::{Config, get_config_path};

//     #[test]
//     fn try_read_config() -> anyhow::Result<()> {
//         // copy config
//         let config_path = get_config_path();
//         std::fs::copy(config_path, env!("CONFIG_FILE"))?;

//         // try read config
//         let config = Config::try_read()?;
//         println!("{config:?}");

//         // clear up tmp files
//         std::fs::remove_file(env!("CONFIG_FILE"))?;

//         Ok(())
//     }
// }
