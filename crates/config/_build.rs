/// Env var that indicates to use the dev config
const DB_TEST_DEV: &str = "DB_TEST_DEV";

const CONFIG_FILE: &str = "bench-config.toml";
const DEV_CONFIG_FILE: &str = "bench-dev-config.toml";

const CONFIG_FILE_DST: &str = "bench-config.toml";

fn main() -> anyhow::Result<()> {
    let config_file = get_config_file()?;

    println!("cargo::rerun-if-env-changed={DB_TEST_DEV}");
    println!("cargo::rerun-if-changed={}", config_file.display());
    println!("cargo::rustc-env=CONFIG_FILE={CONFIG_FILE_DST}");

    copy_config_to_dst(config_file.as_path())?;

    Ok(())
}

fn get_config_file() -> anyhow::Result<std::path::PathBuf> {
    let config_file: &str;

    if std::env::var(DB_TEST_DEV).is_ok() {
        // Use dev config
        config_file = DEV_CONFIG_FILE;
    } else {
        // Use normal config
        config_file = CONFIG_FILE;
    }

    let root_path = std::env::var("CARGO_MANIFEST_DIR")?;
    let root_path = std::path::PathBuf::from(root_path);

    Ok(root_path.join(config_file))
}

fn copy_config_to_dst(config_file: &std::path::Path) -> anyhow::Result<()> {
    let target_dir = get_output_path()?;
    let config_dst_name = std::path::Path::new(CONFIG_FILE_DST);
    let config_dst = target_dir.join(&config_dst_name);

    println!("cargo::rerun-if-changed={}", config_dst.display());
    println!("cargo::rustc-env=CONFIG_FILE_PATH={}", config_dst.display());

    std::fs::copy(config_file, &config_dst)?;

    Ok(())
}

fn get_output_path() -> anyhow::Result<std::path::PathBuf> {
    let out_path = std::env::var("OUT_DIR")?;
    let mut out_path = std::path::PathBuf::from(out_path);

    // OUT_DIR form is:
    // .../target/{profile}/build/db-test-config-{unique_build}/out
    for _ in 0..3 {
        // drop /build/db-test-config-{unique_build}/out
        out_path.pop();
    }

    Ok(out_path)
}
