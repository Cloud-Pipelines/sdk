from collections import OrderedDict
from typing import Mapping, Union


from .. import components
from ..components import structures
from . import artifact_stores
from . import launchers


def run_container_tasks(
    task_specs: OrderedDict[str, structures.TaskSpec],
    graph_input_arguments: Mapping[str, Union[str, artifact_stores.Artifact]],
    task_launcher: launchers.ContainerTaskLauncher,
    artifact_store: artifact_stores.ArtifactStore,
):
    graph_input_arguments = graph_input_arguments or {}

    graph_input_artifacts = {
        input_name: (
            argument
            if isinstance(argument, artifact_stores.Artifact)
            else artifact_store.upload_text(argument)
        )
        for input_name, argument in graph_input_arguments.items()
    }

    task_id_to_output_artifacts_map = {}  # Task ID -> output name -> artifact

    for task_id, task in task_specs.items():
        # resolving task arguments
        task_input_artifacts = {}
        for input_name, argument in task.arguments.items():
            artifact: artifact_stores.Artifact = None
            if isinstance(argument, str):
                artifact = artifact_store.upload_text(text=argument)
            elif isinstance(argument, structures.GraphInputArgument):
                artifact = graph_input_artifacts[argument.graph_input.input_name]
            elif isinstance(argument, structures.TaskOutputArgument):
                artifact = task_id_to_output_artifacts_map[
                    argument.task_output.task_id
                ][argument.task_output.output_name]
            else:
                raise TypeError(
                    "Unsupported argument type: {} - {}.".format(
                        str(type(argument).__name__), str(argument)
                    )
                )
            task_input_artifacts[input_name] = artifact

        if task.component_ref.spec is None:
            if task.component_ref.url:
                task.component_ref.spec = components.load_component_from_url(
                    task.component_ref.url
                ).component_spec
            else:
                raise RuntimeError(
                    f'Cannot get task spec for the "{task_id}" task from component reference: {task.component_ref}.'
                )

        output_artifacts = task_launcher.launch_container_task(
            task_spec=task,
            artifact_store=artifact_store,
            input_artifacts=task_input_artifacts,
        )

        task_id_to_output_artifacts_map[task_id] = output_artifacts
