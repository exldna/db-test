fn main() -> anyhow::Result<()> {
    let qualities = (1_000_000..10_000_000).step_by(1_000_000);
    db_test_model::generate_data(qualities)?;
    Ok(())
}
