use db_test_model::temp::RespFilesManager;

use crate::docker::Docker;

pub struct RedisInsertBulk {
    docker: bollard::Docker,
    containers_pool: crate::docker::Pool<Self>,
}

// Note: this reimport is private and exists only for consistent naming
use RedisInsertBulk as Backend;

impl crate::docker::Docker for Backend {
    const IMAGE_NAME: &'static str = "redis";
    const CONTAINER_NAME_PREFIX: &'static str = "bench-redis-insert-bulk";
}

struct Commander;

impl Commander {
    const BULK_FILE_DIR: &str = "/tmp";
    const BULK_FILE: &str = const_format::formatc!("{}/items", Commander::BULK_FILE_DIR);

    fn redis_insert_piped() -> Vec<&'static str> {
        const PIPED_INSERT: &str = // "redis-cli PING";
            const_format::formatc!("cat {} | redis-cli --pipe", Commander::BULK_FILE);

        vec!["bash", "-c", PIPED_INSERT]
    }
}

impl crate::Backend for Backend {
    type Input = crate::InsertBulkInput;
    type Bencher = crate::docker::Bench<Self, crate::InsertBulkInput>;

    async fn setup(docker: bollard::Docker) -> Self {
        let containers_pool = crate::docker::Pool::new(docker.clone());
        Backend {
            docker,
            containers_pool,
        }
    }

    #[allow(refining_impl_trait)]
    async fn prepare(&self, input: &Self::Input) -> anyhow::Result<Self::Bencher> {
        // Create container
        let container_info = self.containers_pool.create_container().await?;
        let crate::docker::ContainerInfo { container_name, .. } = container_info;

        let container_guard = {
            let container_name = container_name.clone();
            Backend::start_container(&self.docker, container_name).await?
        };

        // Upload bulk file
        let dst_file = std::path::Path::new(Commander::BULK_FILE);
        let dst_path = std::path::PathBuf::from(Commander::BULK_FILE_DIR);
        let tar_path = RespFilesManager::tar_data_file(&input.file_path, dst_file)?;
        Backend::upload_large_file(&self.docker, &container_name, tar_path, dst_path).await?;

        // Prepare bench exec
        let command = Commander::redis_insert_piped();
        let exec_id = Backend::create_exec(&self.docker, &container_name, command).await?;

        Ok(crate::docker::Bench::new(
            self.docker.clone(),
            exec_id,
            container_guard,
        ))
    }
}

#[cfg(test)]
mod tests {
    // use super::*;
    // use crate::{Backend, Bencher, Context};

    // #[test]
    // fn run_bench() -> anyhow::Result<()> {
    //     let Context {
    //         runtime, backend, ..
    //     } = Context::<RedisInsertBulk>::new()?;
    //     let csv_file_path = gen_test_csv("test_redis_run_bench")?;
    //     let bench_input = crate::InsertBulkInput {
    //         file_path: csv_file_path,
    //     };
    //     let prepare_bench = async {
    //         let bench = backend.prepare(&bench_input).await?;
    //         Ok::<_, anyhow::Error>(bench)
    //     };
    //     let bench = runtime.block_on(prepare_bench)?;
    //     let _enter = runtime.enter();
    //     let _guard = runtime.block_on(bench.run());
    //     std::fs::remove_file(bench_input.file_path)?;
    //     std::mem::forget(_guard);
    //     Ok(())
    // }

    // #[tokio::test]
    // async fn file_upload() -> anyhow::Result<()> {
    //     use RedisInsertBulk as Backend;
    //     let container_name = "test-redis-insert-bulk-file-upload".to_owned();
    //     let docker = bollard::Docker::connect_with_local_defaults()?;
    //     Backend::create_container(&docker, container_name.as_str()).await?;
    //     let container_guard = {
    //         let container_name = container_name.clone().into_boxed_str();
    //         Backend::start_container(&docker, container_name).await?
    //     };
    //     let csv_file_path = gen_test_csv("test_redis_file_upload")?;
    //     let tar_file_path =
    //         RespFilesManager::tar_data_file(&csv_file_path, &Commander::bulk_file_dest_folder())?;
    //     let dest_path = Commander::bulk_file_dest_folder();
    //     Backend::upload_large_file(&docker, &container_name, tar_file_path, dest_path).await?;
    //     std::mem::forget(container_guard);
    //     Ok(())
    // }

    // fn gen_test_csv(csv_file_name: &str) -> anyhow::Result<std::path::PathBuf> {
    //     let csv_file_name = format!("{csv_file_name}.csv");
    //     let csv_file_dir = std::env::temp_dir().join("db-test");
    //     let _ = std::fs::create_dir_all(&csv_file_dir);
    //     let csv_file_path = csv_file_dir.join(csv_file_name);
    //     let transactions = [
    //         (
    //             "1PuJjnF476W3zXfVYmJfGnouzFDAXakkL4",
    //             1742574716,
    //             "577d184106faaafec8a95c3ac9287b914423944667bdf3178a98ddd3af34aaed",
    //         ),
    //         (
    //             "17oDqnkSHeU7Gg4vsXiRVcvCci2YKa7hLg",
    //             1742574716,
    //             "9cad2f3a37de9ce804dfdf6249a2dbce90b429eec74f7a6172c6f9b125e95a64",
    //         ),
    //         (
    //             "17oDqnkSHeU7Gg4vsXiRVcvCci2YKa7hLg",
    //             1742574716,
    //             "9cad2f3a37de9ce804dfdf6249a2dbce90b429eec74f7a6172c6f9b125e95a64",
    //         ),
    //     ];
    //     let mut csv_writer = csv::Writer::from_path(&csv_file_path)?;
    //     for transaction in transactions {
    //         csv_writer.write_record(&[
    //             transaction.0,
    //             transaction.1.to_string().as_str(),
    //             transaction.2,
    //         ])?;
    //     }
    //     Ok(csv_file_path)
    // }
}
