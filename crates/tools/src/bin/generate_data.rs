use clap::Parser;

fn main() -> anyhow::Result<()> {
    let config = Config::parse();
    let data_dir = &config.data_dir;
    let qualities = config.qualities.iter().copied();
    std::fs::create_dir_all(data_dir)?;
    db_test_model::generate_data(data_dir, qualities)?;
    Ok(())
}

#[derive(Clone, Debug, Parser)]
#[command()]
struct Config {
    #[arg()]
    data_dir: Box<std::path::Path>,
    #[arg()]
    qualities: Vec<usize>,
}
