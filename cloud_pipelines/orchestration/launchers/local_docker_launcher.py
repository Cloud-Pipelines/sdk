import os
from pathlib import Path, PurePosixPath
import shutil
import tempfile
from typing import Callable

import docker

from ...components import structures
from ..._components.components._components import _resolve_command_line_and_paths

from .naming_utils import sanitize_file_name


class DockerContainerLauncher:
    def launch_container_task(
        self,
        task_spec: structures.TaskSpec,
        download: Callable[[str], str],
        upload: Callable[[str], str],
        input_uris_map: dict = None,
        output_uris_map: dict = None,
    ):
        input_uris_map = input_uris_map or {}
        output_uris_map = output_uris_map or {}

        input_names = list(input_uris_map.keys())
        output_names = list(output_uris_map.keys())

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

            # Getting input data
            for input_name, input_uri in input_uris_map.items():
                input_host_path = host_input_paths_map[input_name]
                Path(input_host_path).parent.mkdir(parents=True, exist_ok=True)
                download(input_uri, input_host_path)

            for output_host_path in host_output_paths_map.values():
                Path(output_host_path).parent.mkdir(parents=True, exist_ok=True)

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

            component_spec = task_spec.component_ref.spec

            resolved_cmd = _resolve_command_line_and_paths(
                component_spec=component_spec,
                arguments=task_spec.arguments,
                input_path_generator=container_input_paths_map.get,
                output_path_generator=container_output_paths_map.get,
            )

            container_env = component_spec.implementation.container.env or {}

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
            container_res = docker_client.containers.run(
                image=component_spec.implementation.container.image,
                entrypoint=resolved_cmd.command,
                command=resolved_cmd.args,
                environment=container_env,
                # remove=True,
                volumes=volumes,
            )

            print("Container logs:")
            print(container_res)

            # Storing the output data
            for output_name, output_uri in output_uris_map.items():
                output_host_path = host_output_paths_map[output_name]
                upload(output_host_path, output_uri)
        finally:
            shutil.rmtree(tempdir, ignore_errors=True)
