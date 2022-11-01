#%%
from collections import OrderedDict
from pathlib import Path
import shutil
from urllib import parse as urllib_parse

from .. import components
from ..components import structures
from .launchers.naming_utils import sanitize_file_name


def run_container_tasks(
    task_specs: OrderedDict[str, structures.TaskSpec],
    graph_input_arguments: dict,
    task_launcher,
    output_uri_generator,
):
    graph_input_arguments = graph_input_arguments or {}

    def generate_execution_id_for_task(task_spec):
        return str(id(task_spec))

    # task_id_to_task_map = {}
    task_id_to_output_uris_map = {}  # Task ID -> output name -> path

    for task_id, task in task_specs.items():
        execution_id = generate_execution_id_for_task(task)
        # resolving task arguments
        resolved_argument_values = {}
        resolved_argument_paths = {}
        for input_name, argument in task.arguments.items():
            resolved_argument_path = None
            resolved_argument_value = None
            # if isinstance(argument, str):
            if isinstance(argument, (str, int, float)):
                resolved_argument_value = str(argument)
                # resolved_argument_path = ???
            elif isinstance(argument, structures.GraphInputArgument):
                resolved_argument_value = graph_input_arguments[
                    argument.graph_input.input_name
                ]
                # resolved_argument_path = ???
            elif isinstance(argument, structures.TaskOutputArgument):
                task_output_uris_map = task_id_to_output_uris_map[
                    argument.task_output.task_id
                ]
                resolved_argument_uri = task_output_uris_map[
                    argument.task_output.output_name
                ]
                # TODO: ! Instead of downloading the input value here, pass an instance of Artifact as an argument.
                # Then those objects can be intercepted and downloaded using a modified serialize_value function wrapper.
                resolved_argument_value = download_string(
                    source_uri=resolved_argument_uri
                )  # TODO: Refactor value resolving to be lazy
            else:
                raise TypeError(
                    "Unsupported argument type: {} - {}.".format(
                        str(type(argument).__name__), str(argument)
                    )
                )

            if resolved_argument_path:
                resolved_argument_paths[input_name] = resolved_argument_path
            if resolved_argument_value:
                resolved_argument_values[input_name] = resolved_argument_value

        if task.component_ref.spec is None:
            if task.component_ref.url:
                task.component_ref.spec = components.load_component_from_url(
                    task.component_ref.url
                ).component_spec
            else:
                raise RuntimeError(
                    f'Cannot get task spec for the "{task_id}" task from component reference: {task.component_ref}.'
                )
        component_spec = task.component_ref.spec

        output_uris_map = {
            output.name: output_uri_generator.generate_execution_output_uri(
                execution_id, output.name
            )
            for output in component_spec.outputs
        }
        task_id_to_output_uris_map[task_id] = output_uris_map

        resolved_task_spec = structures.TaskSpec(
            component_ref=task.component_ref,
            arguments=resolved_argument_values,
        )

        task_launcher.launch_container_task(
            task_spec=resolved_task_spec,
            input_uris_map=resolved_argument_paths,
            output_uris_map=output_uris_map,
            download=download,
            upload=upload,
        )


class LocalPathGenerator:
    def __init__(self, output_root_dir: str):
        self.output_root_dir = output_root_dir

    def generate_execution_output_uri(self, execution_id: str, output_name: str) -> str:
        _single_io_file_name = "data"
        path = str(
            Path(self.output_root_dir)
            / str(execution_id)
            / sanitize_file_name(output_name)
            / _single_io_file_name
        )
        return path


class UriPathGenerator:
    def __init__(self, output_root_dir_uri: str):
        self.output_root_dir_uri = output_root_dir_uri

    def generate_execution_output_uri(self, execution_id: str, output_name: str) -> str:
        _single_io_file_name = "data"
        relative_uri = "/".join(
            str(execution_id), sanitize_file_name(output_name), _single_io_file_name
        )
        uri = urllib_parse.urljoin(self.output_root_dir_uri, relative_uri)
        return uri


def upload(source_local_path: str, destination_uri: str):
    # Only local URIs are supported here now
    dest_path = destination_uri
    Path(dest_path).parent.mkdir(parents=True, exist_ok=True)
    # shutil.copytree(source_local_path, dest_path, symlinks=True)
    print(f"Copy from {source_local_path} to {dest_path}")
    shutil.copy(source_local_path, dest_path, follow_symlinks=False)


def download(source_uri: str, destination_local_path: str):
    # Only local URIs are supported here now
    print(f"Copy from {source_uri} to {destination_local_path}")
    shutil.copytree(source_uri, destination_local_path, symlinks=True)


def download_string(source_uri: str) -> str:
    # Only local URIs are supported here now
    return Path(source_uri).read_text()
