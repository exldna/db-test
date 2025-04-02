use clap::Parser;

fn main() -> anyhow::Result<()> {
    let config = Config::parse();
    let out_dir = &config.out_dir;
    let qualities = config.qualities.iter().copied();
    std::fs::create_dir_all(out_dir)?;
    db_test_model::generate_data(out_dir, qualities)?;
    Ok(())
}

#[derive(Clone, Debug, Parser)]
#[command()]
struct Config {
    #[arg()]
    out_dir: Box<std::path::Path>,
    #[arg()]
    qualities: Vec<usize>,
}
