import datetime
import logging
import os
from pathlib import Path
import subprocess
import tempfile
from typing import Callable, Dict, Mapping, Optional

from ...components import structures
from ..._components.components._components import _resolve_command_line_and_paths

from .. import storage_providers
from . import interfaces
from .naming_utils import sanitize_file_name


_logger = logging.getLogger(__name__)

_PROCESS_ID_LOG_ANNOTATION_KEY = "process_id"


class LocalEnvironmentLauncher(interfaces.ContainerTaskLauncher):
    def launch_container_task(
        self,
        task_spec: structures.TaskSpec,
        input_uri_readers: Mapping[str, storage_providers.UriReader],
        output_uri_writers: Mapping[str, storage_providers.UriWriter],
        on_log_entry_callback: Optional[
            Callable[[interfaces.ProcessLogEntry], None]
        ] = None,
    ) -> interfaces.ContainerExecutionResult:
        component_ref: structures.ComponentReference = task_spec.component_ref
        component_spec: structures.ComponentSpec = component_ref.spec
        container_spec: structures.ContainerSpec = (
            component_spec.implementation.container
        )

        input_names = list(input.name for input in component_spec.inputs or [])
        output_names = list(output.name for output in component_spec.outputs or [])

        with tempfile.TemporaryDirectory() as tempdir:
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

            # Getting artifact values when needed
            # Design options: We could download/mount all artifacts and optionally read from local files,
            # but mounting can be more expensive compared to downloading.
            def artifact_value_getter(
                value: storage_providers.UriReader, type_name: str
            ) -> str:
                assert isinstance(value, storage_providers.UriReader)
                return value.download_as_bytes().decode("utf-8")

            resolved_cmd = _resolve_command_line_and_paths(
                component_spec=component_spec,
                arguments=input_uri_readers,
                input_path_generator=host_input_paths_map.get,
                output_path_generator=host_output_paths_map.get,
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

            process_env = os.environ.copy()
            process_env.update(container_spec.env or {})

            start_time = datetime.datetime.utcnow()
            with subprocess.Popen(
                args=resolved_cmd.command + resolved_cmd.args,
                env=process_env,
                cwd=host_workdir,
                # Write STDERR to the same stream as STDOUT
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
            ) as process:
                _logger.info(f"Started process {process.pid}")
                log = interfaces.ProcessLog()
                for log_line_bytes in process.stdout:
                    log_entry = interfaces.ProcessLogEntry(
                        message_bytes=log_line_bytes,
                        time=datetime.datetime.utcnow(),
                        annotations={_PROCESS_ID_LOG_ANNOTATION_KEY: process.pid},
                    )
                    if on_log_entry_callback:
                        on_log_entry_callback(log_entry=log_entry)
                    log.add_entry(log_entry)
                log.close()
                # Wait should be unnecessary since we read all logs to end before getting here.
                # Popen.__exit__ closes process.stdout and calls process.wait
                # process.wait()

            end_time = datetime.datetime.utcnow()
            exit_code = process.returncode

            # Storing the output data
            succeeded = exit_code == 0
            if succeeded:
                for output_name in output_names:
                    output_host_path = host_output_paths_map[output_name]
                    output_uri_writer = output_uri_writers[output_name]
                    output_uri_writer.upload_from_path(output_host_path)

            execution_result = interfaces.ContainerExecutionResult(
                start_time=start_time,
                end_time=end_time,
                exit_code=exit_code,
                log=log,
            )

            return execution_result
