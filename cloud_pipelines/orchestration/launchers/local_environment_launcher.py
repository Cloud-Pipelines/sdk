import datetime
import os
from pathlib import Path
import subprocess
import tempfile
from typing import Dict, Mapping

from ...components import structures
from ..._components.components._components import _resolve_command_line_and_paths

from .. import artifact_stores
from . import interfaces
from .naming_utils import sanitize_file_name


class LocalEnvironmentLauncher(interfaces.ContainerTaskLauncher):
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
                value: artifact_stores.Artifact, input_type
            ) -> str:
                assert isinstance(value, artifact_stores.Artifact)
                return value.read_text()

            resolved_cmd = _resolve_command_line_and_paths(
                component_spec=component_spec,
                arguments=input_artifacts,
                input_path_generator=host_input_paths_map.get,
                output_path_generator=host_output_paths_map.get,
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

            process_env = os.environ.copy()
            process_env.update(container_spec.env or {})

            start_time = datetime.datetime.utcnow()
            res = subprocess.run(
                args=resolved_cmd.command + resolved_cmd.args,
                env=process_env,
                cwd=host_workdir,
                # Write STDERR to the same stream as STDOUT
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
            )
            end_time = datetime.datetime.utcnow()
            logs = res.stdout
            exit_code = res.returncode

            print("Logs:")
            print(logs)

            # Storing the output data
            output_artifacts = None
            succeeded = exit_code == 0
            if succeeded:
                output_artifacts = {}
                for output_name in output_names:
                    output_host_path = host_output_paths_map[output_name]
                    artifact = artifact_store.upload(path=output_host_path)
                    output_artifacts[output_name] = artifact

            execution_result = interfaces.ContainerExecutionResult(
                start_time=start_time,
                end_time=end_time,
                exit_code=exit_code,
                logs=logs,
                output_artifacts=output_artifacts,
            )

            return execution_result
