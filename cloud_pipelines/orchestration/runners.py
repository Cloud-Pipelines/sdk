from concurrent import futures
import dataclasses
import datetime
import enum
import logging
from typing import Any, Callable, Mapping, Optional, Sequence, Union


from .. import components
from ..components import structures
from . import artifact_stores
from . import launchers


_default_log_printer_logger = logging.getLogger(__name__ + "._default_log_printer")
_default_log_printer_logger.setLevel("INFO")
_default_log_printer_logger.propagate = False
_default_log_printer_handler = logging.StreamHandler()
_default_log_printer_handler.setFormatter(logging.Formatter(fmt="%(message)s"))
_default_log_printer_logger.addHandler(_default_log_printer_handler)


def _run_graph_task(
    task_spec: structures.TaskSpec,
    graph_input_artifacts: Mapping[str, artifact_stores.Artifact],
    task_launcher: launchers.ContainerTaskLauncher,
    artifact_store: artifact_stores.ArtifactStore,
    futures_executor: futures.Executor = None,
    on_log_entry_callback: Optional[Callable[[launchers.ProcessLogEntry], None]] = None,
) -> "GraphExecution":
    task_id_to_output_artifacts_map = {}  # Task ID -> output name -> artifact

    component_ref: structures.ComponentReference = task_spec.component_ref
    component_spec: structures.ComponentSpec = component_ref.spec
    graph_spec: structures.GraphSpec = component_spec.implementation.graph
    toposorted_tasks = graph_spec._toposorted_tasks

    graph_output_artifacts = {}
    task_executions = {}
    graph_execution = GraphExecution(
        task_spec=task_spec,
        input_arguments=graph_input_artifacts,
        outputs=graph_output_artifacts,
        task_executions=task_executions,
    )

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

        child_task_execution = run_task(
            task_spec=child_task_spec,
            input_arguments=task_input_artifacts,
            task_launcher=task_launcher,
            artifact_store=artifact_store,
            futures_executor=futures_executor,
            on_log_entry_callback=on_log_entry_callback,
        )
        task_executions[child_task_id] = child_task_execution

        task_id_to_output_artifacts_map[child_task_id] = child_task_execution.outputs

    graph_execution._waiters = [
        task_execution.wait_for_completion
        for task_execution in graph_execution.task_executions.values()
    ]

    # Preparing graph outputs
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

    return graph_execution


def _default_log_printer(log_entry: launchers.ProcessLogEntry):
    log_text = str(log_entry).replace("\r\n", "\n").rstrip()
    _default_log_printer_logger.info(log_text, extra=log_entry.annotations)


def run_task(
    task_spec: structures.TaskSpec,
    input_arguments: Mapping[str, Union[str, artifact_stores.Artifact]],
    task_launcher: launchers.ContainerTaskLauncher,
    artifact_store: artifact_stores.ArtifactStore,
    futures_executor: futures.Executor = None,
    on_log_entry_callback: Optional[
        Callable[[launchers.ProcessLogEntry], None]
    ] = _default_log_printer,
) -> "Execution":
    futures_executor = futures_executor or futures.ThreadPoolExecutor()

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
        output_names = [
            output_spec.name for output_spec in component_spec.outputs or []
        ]
        output_artifact_futures = {
            output_name: futures.Future() for output_name in output_names
        }
        output_future_artifacts = {
            output_name: FutureArtifact(future)
            for output_name, future in output_artifact_futures.items()
        }

        start_time = datetime.datetime.utcnow()
        execution = ContainerExecution(
            start_time=start_time,
            task_spec=task_spec,
            input_arguments=input_artifacts,
            status=ExecutionStatus.WaitingForUpstream,
            outputs=output_future_artifacts,
        )

        def launch_container_task_and_set_output_artifact_futures():
            # We might not need to resolve the artifacts explicitly before calling the task launcher,
            # but this makes the error handling easier and avoid exposing the Execution class to the launchers.
            try:
                resolved_input_artifacts = {
                    input_name: (
                        argument.get_artifact()
                        if isinstance(argument, FutureArtifact)
                        else argument
                    )
                    for input_name, argument in input_artifacts.items()
                }
                execution.input_arguments = resolved_input_artifacts
            except (UpstreamExecutionFailedError, ExecutionFailedError) as e:
                execution.status = ExecutionStatus.UpstreamFailed
                failed_upstream_execution = (
                    e.upstream_execution
                    if isinstance(e, UpstreamExecutionFailedError)
                    else e.execution
                )
                execution._failed_upstream_execution = failed_upstream_execution
                for future in output_artifact_futures.values():
                    future.set_exception(
                        UpstreamExecutionFailedError(
                            execution=execution,
                            upstream_execution=failed_upstream_execution,
                        )
                    )
                return None

            execution.status = ExecutionStatus.Running
            if on_log_entry_callback:
                log_text = f"Starting container task."
                log_entry = launchers.ProcessLogEntry(
                    message_bytes=log_text.encode("utf-8"),
                    time=datetime.datetime.utcnow(),
                )
                on_log_entry_callback(log_entry)

            container_execution_result = task_launcher.launch_container_task(
                task_spec=task_spec,
                input_artifacts=resolved_input_artifacts,
                artifact_store=artifact_store,
                on_log_entry_callback=on_log_entry_callback,
            )
            execution.end_time = container_execution_result.end_time
            if container_execution_result.exit_code == 0:
                execution.status = ExecutionStatus.Succeeded
                output_artifacts = container_execution_result.output_artifacts
                for output_name, future in output_artifact_futures.items():
                    future.set_result(output_artifacts[output_name])
            else:
                execution.status = ExecutionStatus.Failed
                for future in output_artifact_futures.values():
                    future.set_exception(ExecutionFailedError(execution=execution))
            if on_log_entry_callback:
                log_text = (
                    f"Container task completed with status: {execution.status.name}"
                )
                log_entry = launchers.ProcessLogEntry(
                    message_bytes=log_text.encode("utf-8"),
                    time=datetime.datetime.utcnow(),
                )
                on_log_entry_callback(log_entry)
            return container_execution_result

        container_launch_future = futures_executor.submit(
            launch_container_task_and_set_output_artifact_futures
        )
        execution._waiters = [container_launch_future.result]
        # execution._container_launch_future = container_launch_future
        return execution
    elif isinstance(component_spec.implementation, structures.GraphImplementation):
        return _run_graph_task(
            task_spec=task_spec,
            graph_input_artifacts=input_artifacts,
            task_launcher=task_launcher,
            artifact_store=artifact_store,
            on_log_entry_callback=on_log_entry_callback,
        )
    else:
        raise RuntimeError(
            f"Unsupported component implementation: {component_spec.implementation}"
        )


_eager_mode: Optional["EagerMode"] = None


def enable_eager_mode(
    task_launcher: launchers.ContainerTaskLauncher = None,
    artifact_store: artifact_stores.ArtifactStore = None,
    wait_for_completion_on_exit: bool = True,
):
    global _eager_mode
    if _eager_mode:
        raise RuntimeError("Already in eager mode.")

    _eager_mode = EagerMode(
        task_launcher=task_launcher,
        artifact_store=artifact_store,
        wait_for_completion_on_exit=wait_for_completion_on_exit,
    )
    _eager_mode.__enter__()


def disable_eager_mode():
    global _eager_mode
    if not _eager_mode:
        raise RuntimeError("Not in eager mode.")
    _eager_mode.__exit__(None, None, None)
    _eager_mode = None


class EagerMode:
    def __init__(
        self,
        task_launcher: launchers.ContainerTaskLauncher = None,
        artifact_store: artifact_stores.ArtifactStore = None,
        wait_for_completion_on_exit: bool = True,
    ):
        if not task_launcher:
            from .launchers.local_docker_launcher import DockerContainerLauncher

            task_launcher = DockerContainerLauncher()
        if not artifact_store:
            from .artifact_stores.local_artifact_store import LocalArtifactStore
            import tempfile

            root_dir = tempfile.mkdtemp(prefix="cloud_pipelines.")
            artifact_store = LocalArtifactStore(root_dir)
        self._task_launcher = task_launcher
        self._artifact_store = artifact_store
        self._old_container_task_constructor = None
        self._wait_for_completion_on_exit = wait_for_completion_on_exit
        self._executions: Sequence[Execution] = []

    def __enter__(self):
        def _create_execution_from_component_and_arguments(
            arguments: Mapping[str, Any],
            component_ref: structures.ComponentReference = None,
            **kwargs,
        ) -> Execution:
            task_spec = structures.TaskSpec(
                component_ref=component_ref,
            )
            execution = run_task(
                task_spec=task_spec,
                input_arguments=arguments,
                artifact_store=self._artifact_store,
                task_launcher=self._task_launcher,
            )
            self._executions.append(execution)
            return execution

        from .._components.components import _components

        self._old_container_task_constructor = _components._container_task_constructor
        _components._container_task_constructor = (
            _create_execution_from_component_and_arguments
        )

    def __exit__(self, exc_type, exc_value, traceback):
        from .._components.components import _components

        _components._container_task_constructor = self._old_container_task_constructor
        if self._wait_for_completion_on_exit:
            for execution in self._executions:
                execution.wait_for_completion()


class ExecutionStatus(enum.Enum):
    Invalid = 0
    WaitingForUpstream = 1
    Starting = 2
    Running = 3
    Succeeded = 4
    Failed = 5
    UpstreamFailed = 6
    ConditionallySkipped = 7


@dataclasses.dataclass
class Execution:
    task_spec: structures.TaskSpec
    input_arguments: Mapping[str, artifact_stores.Artifact]
    outputs: Optional[Mapping[str, artifact_stores.Artifact]] = None
    _waiters: Sequence[Callable[[], None]] = None

    def wait_for_completion(self):
        for waiter in self._waiters or []:
            waiter()
        self._waiters = []


@dataclasses.dataclass
class ContainerExecution(Execution):
    status: ExecutionStatus = ExecutionStatus.Invalid
    start_time: Optional[datetime.datetime] = None
    end_time: Optional[datetime.datetime] = None
    # TODO: Launcher-specific info


@dataclasses.dataclass
class GraphExecution(Execution):
    task_executions: Mapping[str, Execution] = None


class ExecutionFailedError(Exception):
    def __init__(self, execution):
        self.execution = execution


class UpstreamExecutionFailedError(Exception):
    def __init__(self, execution, upstream_execution):
        self.execution = execution
        self.upstream_execution = upstream_execution


class FutureArtifact(artifact_stores.Artifact):
    def __init__(self, artifact_future: futures.Future[artifact_stores.Artifact]):
        self._artifact_future = artifact_future

    def get_artifact(self) -> artifact_stores.Artifact:
        self._artifact_future.running
        return self._artifact_future.result()

    def download(self, path: str):
        self.get_artifact().download(path=path)

    def read_text(self) -> str:
        return self.get_artifact().read_text()
