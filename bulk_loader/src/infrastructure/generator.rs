use crate::application::ports::SstGenerator;
use crate::config::Config;

pub struct RocksSstGenerator {
    // ...
}

impl RocksSstGenerator {
    pub fn new(config: &Config) -> Self {
        // ...
        Self {}
    }
}

impl SstGenerator for RocksSstGenerator {
    fn run(&self) -> anyhow::Result<()> {
        // TODO: Реализовать логику генерации SST-файлов
        tracing::info!("(STUB) Running SST generator...");
        Ok(())
    }
}
