use crate::application::ports::Sorter;
use crate::config::Config;

pub struct DataFusionSorter {
    // Пока оставим пустым, добавим поля позже
}

impl DataFusionSorter {
    pub fn new(config: &Config) -> Self {
        // ...
        Self {}
    }
}

impl Sorter for DataFusionSorter {
    fn run(&self) -> anyhow::Result<()> {
        // TODO: Вставить сюда нашу реализацию на DataFusion
        tracing::info!("(STUB) Running DataFusion sorter...");
        Ok(())
    }
}
