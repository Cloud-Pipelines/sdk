import datetime
import os
from pathlib import Path, PurePosixPath
import shutil
import tempfile
from typing import Dict, Mapping

import docker

from ...components import structures
from ..._components.components._components import _resolve_command_line_and_paths

from .. import artifact_stores
from . import interfaces
from .naming_utils import sanitize_file_name


class DockerContainerLauncher(interfaces.ContainerTaskLauncher):
    def launch_container_task(
        self,
        task_spec: structures.TaskSpec,
        artifact_store: artifact_stores.ArtifactStore,
        input_artifacts: Mapping[str, artifact_stores.Artifact] = None,
    ) -> interfaces.ContainerExecutionResult:
        component_ref: structures.ComponentReference = task_spec.component_ref
        component_spec: structures.ComponentSpec = component_ref.spec
        container_spec: structures.ContainerSpec = (
            component_spec.implementation.container
        )

        input_artifacts = input_artifacts or {}

        input_names = list(input.name for input in component_spec.inputs or [])
        output_names = list(output.name for output in component_spec.outputs or [])

        # Not using with tempfile.TemporaryDirectory() as tempdir: due to: OSError: [WinError 145] The directory is not empty
        # Python 3.10 supports the ignore_cleanup_errors=True parameter.
        try:
            tempdir = tempfile.mkdtemp()

            host_workdir = os.path.join(tempdir, "work")
            # host_logdir = os.path.join(tempdir, 'logs')
            host_input_paths_map = {
                name: os.path.join(tempdir, "inputs", sanitize_file_name(name), "data")
                for name in input_names
            }  # Or just use random temp dirs/subdirs
            host_output_paths_map = {
                name: os.path.join(tempdir, "outputs", sanitize_file_name(name), "data")
                for name in output_names
            }  # Or just use random temp dirs/subdirs

            Path(host_workdir).mkdir(parents=True, exist_ok=True)

            container_input_root = PurePosixPath("/tmp/inputs/")
            container_output_root = PurePosixPath("/tmp/outputs/")
            container_input_paths_map = {
                name: str(container_input_root / sanitize_file_name(name) / "data")
                for name in input_names
            }  # Or just user random temp dirs/subdirs
            container_output_paths_map = {
                name: str(container_output_root / sanitize_file_name(name) / "data")
                for name in output_names
            }  # Or just user random temp dirs/subdirs

            # Getting artifact values when needed
            # Design options: We could download/mount all artifacts and optionally read from local files,
            # but mounting can be more expensive compared to downloading.
            def artifact_value_getter(
                value: artifact_stores.Artifact, input_type
            ) -> str:
                assert isinstance(value, artifact_stores.Artifact)
                return value.read_text()

            resolved_cmd = _resolve_command_line_and_paths(
                component_spec=component_spec,
                arguments=input_artifacts,
                input_path_generator=container_input_paths_map.get,
                output_path_generator=container_output_paths_map.get,
                argument_serializer=artifact_value_getter,
            )

            # Getting the input data
            for input_name in resolved_cmd.input_paths.keys():
                input_host_path = host_input_paths_map[input_name]
                input_artifact = input_artifacts[input_name]
                Path(input_host_path).parent.mkdir(parents=True, exist_ok=True)
                input_artifact.download(path=input_host_path)

            # Preparing the output locations
            for output_host_path in host_output_paths_map.values():
                Path(output_host_path).parent.mkdir(parents=True, exist_ok=True)

            container_env = container_spec.env or {}

            volumes = {}
            for input_name in input_names:
                host_dir = os.path.dirname(host_input_paths_map[input_name])
                container_dir = os.path.dirname(container_input_paths_map[input_name])
                volumes[host_dir] = dict(
                    bind=container_dir,
                    # mode='ro',
                    mode="rw",  # We're copying the input data anyways, so it's OK if the container modifies it.
                )
            for output_name in output_names:
                host_dir = os.path.dirname(host_output_paths_map[output_name])
                container_dir = os.path.dirname(container_output_paths_map[output_name])
                volumes[host_dir] = dict(
                    bind=container_dir,
                    mode="rw",
                )

            docker_client = docker.from_env()
            start_time = datetime.datetime.utcnow()
            container = docker_client.containers.run(
                image=component_spec.implementation.container.image,
                entrypoint=resolved_cmd.command,
                command=resolved_cmd.args,
                environment=container_env,
                # remove=True,
                volumes=volumes,
                detach=True,
            )
            log_generator = container.logs(
                stdout=True, stderr=True, stream=True, follow=True
            )
            log_lines = []
            for log_line in log_generator:
                log_lines.append(log_line)
                print(log_line)
            wait_result = container.wait()
            end_time = datetime.datetime.utcnow()

            exit_status = wait_result["StatusCode"]
            # logs_bytes = container.logs(stdout=True, stderr=True, stream=False)
            logs_bytes = b"".join([line for line in log_lines])

            print("Container logs:")
            print(logs_bytes)

            # Storing the output data
            output_artifacts = None
            succeeded = exit_status == 0
            if succeeded:
                output_artifacts = {}
                for output_name in output_names:
                    output_host_path = host_output_paths_map[output_name]
                    artifact = artifact_store.upload(path=output_host_path)
                    output_artifacts[output_name] = artifact

            execution_result = interfaces.ContainerExecutionResult(
                start_time=start_time,
                end_time=end_time,
                exit_code=exit_status,
                logs=logs_bytes,
                output_artifacts=output_artifacts,
            )

            return execution_result
        finally:
            shutil.rmtree(tempdir, ignore_errors=True)
