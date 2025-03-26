use crate::docker::Docker;

pub struct PostgresInsertBulk {
    containers_pool: crate::docker::Pool<Self>,
}

impl crate::docker::Docker for PostgresInsertBulk {
    const IMAGE_NAME: &'static str = "postgres";
    const CONTAINER_NAME_PREFIX: &'static str = "bench-postgres-insert-bulk";

    async fn create_container(
        docker: &bollard::Docker,
        container_name: &str,
    ) -> anyhow::Result<bollard::secret::ContainerCreateResponse> {
        let options = bollard::container::CreateContainerOptions {
            name: container_name,
            platform: None,
        };
        let config = bollard::container::Config {
            image: Some(Self::IMAGE_NAME),
            env: Some(vec!["POSTGRES_HOST_AUTH_METHOD=trust"]),
            ..Default::default()
        };
        let container = docker.create_container(Some(options), config).await?;
        Ok(container)
    }
}

impl crate::Backend for PostgresInsertBulk {
    type Input = crate::InsertBulkBenchInput;

    async fn setup(docker: &bollard::Docker) -> Self {
        let containers_pool = crate::docker::Pool::new(docker.clone());
        PostgresInsertBulk { containers_pool }
    }

    #[rustfmt::skip]
    #[allow(refining_impl_trait)]
    async fn prepare(&self, docker: &bollard::Docker) -> PostgresInsertBulkBench {
        let container_info = self.containers_pool.create_container()
            .await
            .expect("cannot create postgres container");
        let conatiner_name = container_info.container_name.clone();
        let container_guard = PostgresInsertBulk::start_container(docker, conatiner_name)
            .await
            .expect("cannot start postgres container");
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        PostgresInsertBulkBench::new(container_info, container_guard)
    }
}

pub struct PostgresInsertBulkBench {
    container_info: crate::docker::ContainerInfo,
    container_guard: Option<crate::docker::ContainerGuard>,
    bench_exec_id: Option<crate::docker::ExecId>,
}

impl PostgresInsertBulkBench {
    const BULK_FILE: &str = "/tmp/items";

    fn new(
        container_info: crate::docker::ContainerInfo,
        container_guard: crate::docker::ContainerGuard,
    ) -> Self {
        let container_guard = Some(container_guard);
        PostgresInsertBulkBench {
            container_info,
            container_guard,
            bench_exec_id: None,
        }
    }

    async fn migrate(docker: &bollard::Docker, container_name: &str) {
        for migration in sqlx::migrate!().iter() {
            let migration_exec = {
                let migration_command = format!("psql -c \"{}\" -U postgres", migration.sql);
                let command = vec!["bash", "-c", migration_command.as_str()];
                PostgresInsertBulk::create_exec(docker, container_name, command)
                    .await
                    .expect("cannot create postgres prepare exec")
            };
            let migration_exec_id = crate::docker::ExecId(migration_exec.id.into_boxed_str());
            let migration_exec_attach = PostgresInsertBulk::start_exec(docker, &migration_exec_id)
                .await
                .expect("cannot start postgres migration command");
            PostgresInsertBulk::attached_exec(migration_exec_attach).await;
        }
    }

    async fn create_bulk_file(docker: &bollard::Docker, items_count: u64, container_name: &str) {
        let prepare_exec = {
            let repeat_insert = format!(
                "yes \'qwerty 1742392583 qwerty\' | head -{} > {}",
                items_count,
                Self::BULK_FILE
            );
            let exec_command = vec!["bash", "-c", repeat_insert.as_str()];
            PostgresInsertBulk::create_exec(docker, container_name, exec_command)
                .await
                .expect("cannot create postgres prepare exec")
        };
        let prepare_exec_id = crate::docker::ExecId(prepare_exec.id.into_boxed_str());
        let prepare_exec_attach = PostgresInsertBulk::start_exec(docker, &prepare_exec_id)
            .await
            .expect("cannot start redis insert bulk command");
        PostgresInsertBulk::attached_exec(prepare_exec_attach).await;
    }
}

impl crate::Bench for PostgresInsertBulkBench {
    type Input = crate::InsertBulkBenchInput;

    async fn run(&mut self, input: Self::Input) -> crate::docker::ContainerGuard {
        let exec_id = self.bench_exec_id.as_ref().unwrap();
        let attach = PostgresInsertBulk::start_exec(&input.docker, exec_id)
            .await
            .expect("cannot start postgres insert bulk command");
        PostgresInsertBulk::attached_exec(attach).await;
        self.container_guard.take().unwrap()
    }

    async fn prepare(&mut self, input: &Self::Input) -> () {
        let container_name = &self.container_info.container_name;
        Self::migrate(&input.docker, container_name).await;
        Self::create_bulk_file(&input.docker, input.items_count, container_name).await;
        let bench_exec = {
            let insert_query ="COPY user_transactions FROM '/tmp/items' WITH (FORMAT TEXT);";
            let insert_command = format!("psql -c \"{insert_query}\" -U postgres");
            let exec_command = vec!["bash", "-c", insert_command.as_str()];
            PostgresInsertBulk::create_exec(&input.docker, container_name, exec_command)
                .await
                .expect("cannot create postgres bench exec")
        };
        self.bench_exec_id = Some(crate::docker::ExecId(bench_exec.id.into_boxed_str()));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Backend, Bench};

    #[test]
    fn run_container() -> anyhow::Result<()> {
        let items_count = 100_000;
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let docker = bollard::Docker::connect_with_local_defaults()
            .expect("cannot connect to docker daemon");
        let backend = runtime.block_on(PostgresInsertBulk::setup(&docker));
        let bench_input = crate::InsertBulkBenchInput {
            docker: docker.clone(),
            items_count,
        };
        let mut bench = tokio::task::block_in_place(|| {
            runtime.block_on(async {
                let mut bench = backend.prepare(&docker).await;
                bench.prepare(&bench_input).await;
                bench
            })
        });
        let _enter = runtime.enter();
        let _guard = runtime.block_on(bench.run(bench_input));
        std::mem::forget(_guard);
        Ok(())
    }
}
