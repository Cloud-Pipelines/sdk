import datetime
import logging
import os
from pathlib import Path, PurePosixPath
import shutil
import tempfile
from typing import Callable, Mapping, Optional

import docker
from docker.models.containers import Container

from ...components import structures
from ..._components.components._components import _resolve_command_line_and_paths

from .. import storage_providers
from . import interfaces
from .naming_utils import sanitize_file_name


_logger = logging.getLogger(__name__)

_CONTAINER_ID_LOG_ANNOTATION_KEY = "container_id"


class DockerContainerLauncher(interfaces.ContainerTaskLauncher):
    """Launcher that uses Docker installed locally"""

    def __init__(
        self,
        client: Optional[docker.DockerClient] = None,
    ):
        try:
            self._docker_client = client or docker.from_env()
        except Exception as ex:
            raise RuntimeError(
                "Docker does not seem to be working."
                " Please make sure that `docker version` executes without errors."
                " Docker can be installed from https://docs.docker.com/get-docker/."
            ) from ex

    def launch_container_task(
        self,
        task_spec: structures.TaskSpec,
        input_uri_readers: Mapping[str, storage_providers.UriReader],
        output_uri_writers: Mapping[str, storage_providers.UriWriter],
        log_uri_writer: storage_providers.UriWriter,
    ) -> interfaces.LaunchedContainer:
        component_ref: structures.ComponentReference = task_spec.component_ref
        component_spec: structures.ComponentSpec = component_ref.spec
        container_spec: structures.ContainerSpec = (
            component_spec.implementation.container
        )

        input_names = list(input.name for input in component_spec.inputs or [])
        output_names = list(output.name for output in component_spec.outputs or [])

        # Not using with tempfile.TemporaryDirectory() as tempdir: due to: OSError: [WinError 145] The directory is not empty
        # Python 3.10 supports the ignore_cleanup_errors=True parameter.
        tempdir = tempfile.mkdtemp()
        try:
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
                value: storage_providers.UriReader, type_name: str
            ) -> str:
                assert isinstance(value, storage_providers.UriReader)
                return value.download_as_text()

            resolved_cmd = _resolve_command_line_and_paths(
                component_spec=component_spec,
                arguments=input_uri_readers,
                input_path_generator=container_input_paths_map.get,
                output_path_generator=container_output_paths_map.get,
                argument_serializer=artifact_value_getter,
            )

            # Getting the input data
            for input_name in resolved_cmd.input_paths.keys():
                input_host_path = host_input_paths_map[input_name]
                input_uri_reader = input_uri_readers[input_name]
                Path(input_host_path).parent.mkdir(parents=True, exist_ok=True)
                input_uri_reader.download_to_path(path=input_host_path)

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

            start_time = datetime.datetime.utcnow()
            container: Container = self._docker_client.containers.run(
                image=component_spec.implementation.container.image,
                entrypoint=resolved_cmd.command,
                command=resolved_cmd.args,
                environment=container_env,
                # remove=True,
                volumes=volumes,
                detach=True,
            )
            _logger.info(f"Started container {container.id}")

            def upload_logs(text_log: str):
                log_uri_writer.upload_from_text(text_log)

            def upload_all_artifacts():
                for output_name in output_names:
                    output_host_path = host_output_paths_map[output_name]
                    output_uri_writer = output_uri_writers[output_name]
                    # Do not fail on non-existing outputs here. It will be caught by the `Runner` class.
                    if os.path.exists(output_host_path):
                        output_uri_writer.upload_from_path(output_host_path)

            def clean_up():
                container.remove()
                shutil.rmtree(tempdir, ignore_errors=True)

            launched_docker_container = _LaunchedDockerContainer(
                container=container,
                upload_all_artifacts=upload_all_artifacts,
                upload_logs=upload_logs,
                clean_up=clean_up,
                start_time=start_time,
            )
            return launched_docker_container
        except:
            # shutil.rmtree(tempdir, ignore_errors=True)
            raise


class _LaunchedDockerContainer(interfaces.LaunchedContainer):
    def __init__(
        self,
        container: Container,
        upload_all_artifacts: Callable[[], None],
        upload_logs: Callable[[str], None],
        clean_up: Callable[[], None],
        start_time: datetime.datetime,
    ):
        self._container = container
        self._upload_all_artifacts = upload_all_artifacts
        self._upload_logs = upload_logs
        self._clean_up = clean_up
        self._start_time = start_time or datetime.datetime.utcnow()
        self._execution_result: Optional[interfaces.ContainerExecutionResult] = None

    def wait_for_completion(
        self,
        on_log_entry_callback: Optional[
            Callable[[interfaces.ProcessLogEntry], None]
        ] = None,
    ) -> interfaces.ContainerExecutionResult:
        # This function is not thread-safe. It can only be called once.
        if self._execution_result:
            # TODO: Stream the logs?
            return self._execution_result
        log_generator = self._container.logs(
            stdout=True, stderr=True, stream=True, follow=True
        )
        log = interfaces.ProcessLog()
        text_log_chunks = []
        for log_bytes in log_generator:
            log_entry_time = datetime.datetime.utcnow()
            log_entry = interfaces.ProcessLogEntry(
                message_bytes=log_bytes,
                time=log_entry_time,
                annotations={_CONTAINER_ID_LOG_ANNOTATION_KEY: self._container.id},
            )
            if on_log_entry_callback:
                on_log_entry_callback(log_entry)
            log.add_entry(log_entry)
            b''.decode()
            text_log_chunks.append(
                log_entry_time.isoformat(sep=" ", timespec="seconds")
                + "\t"
                + log_bytes.decode("utf-8", errors="replace")
            )
        log.close()

        wait_result = self._container.wait()
        end_time = datetime.datetime.utcnow()

        exit_code = wait_result["StatusCode"]

        # Storing the output data
        self._upload_logs("".join(text_log_chunks))
        succeeded = exit_code == 0
        if succeeded:
            self._upload_all_artifacts()

        self._clean_up()

        self._execution_result = interfaces.ContainerExecutionResult(
            start_time=self._start_time,
            end_time=end_time,
            exit_code=exit_code,
            log=log,
        )
        return self._execution_result

    def terminate(self, grace_period_seconds: Optional[int] = None):
        self._container.stop(
            timeout=grace_period_seconds,
        )
        self._clean_up()
