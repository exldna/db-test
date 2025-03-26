const PROJECT_NAME: &str = "db-test";
const OUT_DIR_ENV: &str = "OUT_DIR";

fn main() {
    let out_dir = scratch::path(PROJECT_NAME);
    println!("cargo::rustc-env={OUT_DIR_ENV}={}", out_dir.display());
}
