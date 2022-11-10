from typing import Mapping, Union


from .. import components
from ..components import structures
from . import artifact_stores
from . import launchers


def _run_graph_task(
    task_spec: structures.TaskSpec,
    graph_input_artifacts: Mapping[str, artifact_stores.Artifact],
    task_launcher: launchers.ContainerTaskLauncher,
    artifact_store: artifact_stores.ArtifactStore,
):
    task_id_to_output_artifacts_map = {}  # Task ID -> output name -> artifact

    component_ref: structures.ComponentReference = task_spec.component_ref
    component_spec: structures.ComponentSpec = component_ref.spec
    graph_spec: structures.GraphSpec = component_spec.implementation.graph
    toposorted_tasks = graph_spec._toposorted_tasks

    for child_task_id, child_task_spec in toposorted_tasks.items():
        # resolving task arguments
        task_input_artifacts = {}
        for input_name, argument in child_task_spec.arguments.items():
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

        output_artifacts = run_task(
            task_spec=child_task_spec,
            input_arguments=task_input_artifacts,
            task_launcher=task_launcher,
            artifact_store=artifact_store,
        )

        task_id_to_output_artifacts_map[child_task_id] = output_artifacts

    # Preparing graph outputs
    graph_output_artifacts = {}
    for output_spec in component_spec.outputs or []:
        output_name = output_spec.name
        output_source = graph_spec.output_values[output_name]  # The entry must exist

        if isinstance(output_source, structures.TaskOutputArgument):
            artifact = task_id_to_output_artifacts_map[
                output_source.task_output.task_id
            ][output_source.task_output.output_name]
        else:
            raise NotImplementedError(
                f"Unsupported graph output source: {output_source}"
            )
        graph_output_artifacts[output_name] = artifact

    return graph_output_artifacts


def run_task(
    task_spec: structures.TaskSpec,
    input_arguments: Mapping[str, Union[str, artifact_stores.Artifact]],
    task_launcher: launchers.ContainerTaskLauncher,
    artifact_store: artifact_stores.ArtifactStore,
):
    if task_spec.component_ref.spec is None:
        if task_spec.component_ref.url:
            task_spec.component_ref.spec = components.load_component_from_url(
                task_spec.component_ref.url
            ).component_spec
        else:
            raise RuntimeError(
                f"Cannot get component spec from component reference: {task_spec.component_ref}."
            )
    component_spec = task_spec.component_ref.spec

    input_artifacts = {
        input_name: (
            argument
            if isinstance(argument, artifact_stores.Artifact)
            else artifact_store.upload_text(argument)
        )
        for input_name, argument in input_arguments.items()
    }

    if isinstance(component_spec.implementation, structures.ContainerImplementation):
        output_artifacts = task_launcher.launch_container_task(
            task_spec=task_spec,
            input_artifacts=input_artifacts,
            artifact_store=artifact_store,
        )
    elif isinstance(component_spec.implementation, structures.GraphImplementation):
        output_artifacts = _run_graph_task(
            task_spec=task_spec,
            graph_input_artifacts=input_artifacts,
            task_launcher=task_launcher,
            artifact_store=artifact_store,
        )
    else:
        raise RuntimeError(
            f"Unsupported component implementation: {component_spec.implementation}"
        )

    return output_artifacts
