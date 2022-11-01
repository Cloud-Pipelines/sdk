import os
from pathlib import Path
import subprocess
import tempfile
from typing import Callable

from ...components import structures
from ..._components.components._components import _resolve_command_line_and_paths

from .naming_utils import sanitize_file_name


class LocalEnvironmentLauncher:
    def launch_container_task(
        self,
        task_spec,
        download: Callable[[str], str],
        upload: Callable[[str], str],
        input_uris_map: dict = None,
        output_uris_map: dict = None,
    ):
        input_uris_map = input_uris_map or {}
        output_uris_map = output_uris_map or {}

        with tempfile.TemporaryDirectory() as tempdir:
            host_workdir = os.path.join(tempdir, "work")
            # host_logdir = os.path.join(tempdir, 'logs')
            host_input_paths_map = {
                name: os.path.join(tempdir, "inputs", sanitize_file_name(name), "data")
                for name in input_uris_map.keys()
            }  # Or just use random temp dirs/subdirs
            host_output_paths_map = {
                name: os.path.join(tempdir, "outputs", sanitize_file_name(name), "data")
                for name in output_uris_map.keys()
            }  # Or just use random temp dirs/subdirs

            Path(host_workdir).mkdir(parents=True, exist_ok=True)

            # Getting input data
            for input_name, input_uri in input_uris_map.items():
                input_host_path = host_input_paths_map[input_name]
                Path(input_host_path).parent.mkdir(parents=True, exist_ok=True)
                download(input_uri, input_host_path)

            for output_host_path in host_output_paths_map.values():
                Path(output_host_path).parent.mkdir(parents=True, exist_ok=True)

            component_spec = task_spec.component_ref.spec

            resolved_cmd = _resolve_command_line_and_paths(
                component_spec=component_spec,
                arguments=task_spec.arguments,
                input_path_generator=host_input_paths_map.get,
                output_path_generator=host_output_paths_map.get,
            )

            process_env = os.environ.copy()
            process_env.update(component_spec.implementation.container.env or {})

            res = subprocess.run(
                args=resolved_cmd.command + resolved_cmd.args,
                env=process_env,
                cwd=host_workdir,
            )

            # Storing the output data
            for output_name, output_uri in output_uris_map.items():
                output_host_path = host_output_paths_map[output_name]
                upload(output_host_path, output_uri)

            # print(res)
