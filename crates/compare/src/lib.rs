pub mod backends;
pub mod docker;

pub trait Backend {
    type Input;
    type Bencher: Bencher<Input = Self::Input>;

    fn setup(docker: bollard::Docker) -> impl Future<Output = Self> + Send;

    fn prepare(
        &self,
        input: &Self::Input,
    ) -> impl Future<Output = anyhow::Result<Self::Bencher>> + Send;
}

pub trait Bencher {
    type Input;

    fn run(self) -> impl Future<Output = anyhow::Result<crate::docker::ContainerGuard>> + Send;
}

pub struct Context<B> {
    pub runtime: tokio::runtime::Runtime,
    pub backend: B,
}

impl<B: Backend> Context<B> {
    pub fn new() -> anyhow::Result<Self> {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let docker = bollard::Docker::connect_with_local_defaults()?;
        let backend = runtime.block_on(B::setup(docker));
        Ok(Context { runtime, backend })
    }

    pub fn block<O>(&self, f: impl Future<Output = O>) -> O {
        tokio::task::block_in_place(|| self.runtime.block_on(f))
    }
}

pub struct InsertBulkInput {
    pub file_path: std::path::PathBuf,
}
