use crate::docker::Docker;

pub struct SqliteInsertBulk {
    containers_pool: crate::docker::Pool<Self>,
}

impl crate::docker::Docker for SqliteInsertBulk {
    const IMAGE_NAME: &'static str = "sqlite";
    const CONTAINER_NAME_PREFIX: &'static str = "bench-sqlite-insert-bulk";
}

impl crate::Backend for SqliteInsertBulk {
    type Input = crate::InsertBulkBenchInput;

    async fn setup(docker: &bollard::Docker) -> Self {
        todo!()
    }

    #[allow(refining_impl_trait)]
    async fn prepare(&self, docker: &bollard::Docker) -> SqliteInsertBulkBench {
        todo!()
    }
}

pub struct SqliteInsertBulkBench {}

impl crate::Bench for SqliteInsertBulkBench {
    type Input = crate::InsertBulkBenchInput;

    async fn run(&mut self, input: Self::Input) -> crate::docker::ContainerGuard {
        todo!()
    }

    async fn prepare(&mut self, input: &Self::Input) -> () {
        todo!()
    }
}
