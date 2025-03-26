use futures_util::{StreamExt, TryFutureExt};
use tokio_util::io::ReaderStream;

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct ContainerId(pub(crate) Box<str>);

#[derive(Debug, Clone)]
pub struct ExecId(pub(crate) Box<str>);

#[derive(Debug)]
pub(crate) struct ContainerInfo {
    pub(crate) container_name: Box<str>,
    #[allow(dead_code)]
    pub(crate) container_id: ContainerId,
}

pub trait Docker: Send + Sync + 'static {
    const IMAGE_NAME: &str;
    const CONTAINER_NAME_PREFIX: &str;

    fn create_container(
        docker: &bollard::Docker,
        container_name: &str,
    ) -> impl Future<Output = anyhow::Result<bollard::secret::ContainerCreateResponse>> + Send {
        async move {
            let options = bollard::container::CreateContainerOptions {
                name: container_name,
                platform: None,
            };
            let config = bollard::container::Config {
                image: Some(Self::IMAGE_NAME),
                ..Default::default()
            };
            let container = docker.create_container(Some(options), config).await?;
            Ok(container)
        }
    }

    fn start_container(
        docker: &bollard::Docker,
        container_name: Box<str>,
    ) -> impl Future<Output = anyhow::Result<ContainerGuard>> + Send {
        async move {
            let options = bollard::container::StartContainerOptions {
                ..Default::default()
            };
            docker
                .start_container::<String>(&container_name, Some(options))
                .await?;
            Ok(ContainerGuard::new(container_name, docker))
        }
    }

    fn create_exec(
        docker: &bollard::Docker,
        container_name: &str,
        cmd: Vec<&str>,
    ) -> impl Future<Output = anyhow::Result<ExecId>> + Send {
        async move {
            let config = bollard::exec::CreateExecOptions {
                attach_stdout: Some(true),
                attach_stderr: Some(true),
                cmd: Some(cmd),
                ..Default::default()
            };
            let exec = docker.create_exec(container_name, config).await?;
            Ok(ExecId(exec.id.into_boxed_str()))
        }
    }

    fn start_exec(
        docker: &bollard::Docker,
        exec_id: &ExecId,
    ) -> impl Future<Output = anyhow::Result<bollard::exec::StartExecResults>> + Send {
        async move {
            let options = bollard::exec::StartExecOptions {
                detach: false,
                ..Default::default()
            };
            let attach = docker.start_exec(&exec_id.0, Some(options)).await?;
            Ok(attach)
        }
    }

    fn attached_exec(attach: bollard::exec::StartExecResults) -> impl Future<Output = ()> + Send {
        async move {
            if let bollard::exec::StartExecResults::Attached { mut output, .. } = attach {
                #[cfg(test)]
                while let Some(Ok(msg)) = output.next().await {
                    println!("{msg}");
                }
                #[cfg(not(test))]
                while let Some(Ok(_)) = output.next().await {}
            }
        }
    }

    fn run_cmd(
        docker: &bollard::Docker,
        container_name: &str,
        cmd: Vec<&str>,
    ) -> impl Future<Output = anyhow::Result<()>> + Send {
        async move {
            let exec_id = Self::create_exec(docker, container_name, cmd).await?;
            let attach = Self::start_exec(docker, &exec_id).await?;
            Self::attached_exec(attach).await;
            Ok(())
        }
    }

    fn upload_large_file(
        docker: &bollard::Docker,
        container_name: &str,
        file_path: std::path::PathBuf,
        dest_path: std::path::PathBuf,
    ) -> impl Future<Output = anyhow::Result<()>> + Send {
        async move {
            let file = tokio::fs::File::open(file_path)
                .map_ok(ReaderStream::new)
                .try_flatten_stream()
                .map(|x| x.expect("failed to stream file"));
            let options = bollard::container::UploadToContainerOptions {
                path: dest_path.display().to_string(),
                ..Default::default()
            };
            docker
                .upload_to_container_streaming(container_name, Some(options), file)
                .await?;
            Ok(())
        }
    }
}

pub(crate) struct Pool<D: Docker> {
    running_containers: std::sync::atomic::AtomicU32,
    docker: bollard::Docker,
    // Consume generic param
    _docker_trait: std::marker::PhantomData<D>,
}

impl<D: Docker> Pool<D> {
    pub fn new(docker: bollard::Docker) -> Self {
        Pool {
            running_containers: 0.into(),
            docker,
            _docker_trait: std::marker::PhantomData,
        }
    }

    pub async fn create_container(&self) -> anyhow::Result<ContainerInfo> {
        let container_name = {
            let container_n = self
                .running_containers
                .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
            format!("{}-{}", D::CONTAINER_NAME_PREFIX, container_n).into_boxed_str()
        };
        let container = D::create_container(&self.docker, &container_name).await?;
        let container_id = ContainerId(container.id.into_boxed_str());
        Ok(ContainerInfo {
            container_name,
            container_id,
        })
    }
}

pub struct Bench<D: Docker, I> {
    docker: bollard::Docker,
    exec_id: crate::docker::ExecId,
    container_guard: crate::docker::ContainerGuard,
    // Consume generic params
    _docker_trait: std::marker::PhantomData<D>,
    _bench_input: std::marker::PhantomData<I>,
}

impl<D: Docker, I> Bench<D, I> {
    pub fn new(
        docker: bollard::Docker,
        exec_id: crate::docker::ExecId,
        container_guard: crate::docker::ContainerGuard,
    ) -> Self {
        Bench {
            docker,
            exec_id,
            container_guard,
            _docker_trait: std::marker::PhantomData,
            _bench_input: std::marker::PhantomData,
        }
    }
}

impl<D: Docker, I> crate::Bencher for Bench<D, I> {
    type Input = I;

    fn run(self) -> impl Future<Output = anyhow::Result<crate::docker::ContainerGuard>> + Send {
        let Bench {
            docker,
            exec_id,
            container_guard,
            ..
        } = self;
        async move {
            let attach = D::start_exec(&docker, &exec_id).await?;
            D::attached_exec(attach).await;
            Ok(container_guard)
        }
    }
}

pub struct ContainerGuard {
    container_name: Option<Box<str>>,
    docker: bollard::Docker,
}

impl ContainerGuard {
    pub fn new(container_name: Box<str>, docker: &bollard::Docker) -> Self {
        let container_name = Some(container_name);
        let docker = docker.clone();
        ContainerGuard {
            container_name,
            docker,
        }
    }
}

impl Drop for ContainerGuard {
    fn drop(&mut self) {
        let docker = self.docker.clone();
        // IMPLEMENATION SAFETY:
        // container_name is always Some until the drop occurs.
        let container_name = self.container_name.take().unwrap();
        tokio::task::block_in_place(|| {
            let runtime = tokio::runtime::Handle::current();
            runtime.block_on(async move {
                let options = bollard::container::RemoveContainerOptions {
                    force: true,
                    ..Default::default()
                };
                docker
                    .remove_container(&container_name, Some(options))
                    .await
                    .expect(format!("cannot remove container: {container_name}").as_str());
            })
        });
    }
}
