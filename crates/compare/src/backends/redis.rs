pub mod insert_bulk;

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn run_command_inside_container() -> anyhow::Result<()> {
        use futures_util::stream::StreamExt;

        async fn create_container(
            docker: &bollard::Docker,
            container_name: &str,
        ) -> anyhow::Result<bollard::secret::ContainerCreateResponse> {
            let options = bollard::container::CreateContainerOptions {
                name: container_name,
                platform: None,
            };
            let config = bollard::container::Config {
                image: Some("redis"),
                ..Default::default()
            };
            let container = docker.create_container(Some(options), config).await?;
            Ok(container)
        }

        async fn start_container(
            docker: &bollard::Docker,
            container_id: &str,
        ) -> anyhow::Result<()> {
            let options = bollard::container::StartContainerOptions {
                ..Default::default()
            };
            docker
                .start_container::<String>(&container_id, Some(options))
                .await?;
            Ok(())
        }

        async fn create_exec(
            docker: &bollard::Docker,
            container_id: &str,
        ) -> anyhow::Result<bollard::exec::CreateExecResults> {
            let config = bollard::exec::CreateExecOptions {
                attach_stdout: Some(true),
                attach_stderr: Some(true),
                cmd: Some(vec!["bash", "-c", "sleep 10"]),
                ..Default::default()
            };
            let exec = docker.create_exec(container_id, config).await?;
            Ok(exec)
        }

        async fn start_exec(
            docker: &bollard::Docker,
            exec_id: &str,
        ) -> anyhow::Result<bollard::exec::StartExecResults> {
            let options = bollard::exec::StartExecOptions {
                detach: false,
                ..Default::default()
            };
            let attach = docker.start_exec(exec_id, Some(options)).await?;
            Ok(attach)
        }

        let docker = bollard::Docker::connect_with_local_defaults().unwrap();
        let container_name = format!("example");

        let container = create_container(&docker, &container_name).await?;
        start_container(&docker, &container.id).await?;
        let exec = create_exec(&docker, &container.id).await?;
        let attach = start_exec(&docker, &exec.id).await?;

        if let bollard::exec::StartExecResults::Attached { mut output, .. } = attach {
            while let Some(Ok(_)) = output.next().await {}
        }

        Ok(())
    }
}
