from concurrent import futures
import dataclasses
import datetime
import enum
import logging
import tempfile
import os
import typing
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Union
import uuid


from .. import components
from .._components.components import _structures
from ..components import structures
from . import artifact_stores
from . import storage_providers
from . import launchers

from ..components import _serialization


_default_log_printer_logger = logging.getLogger(__name__ + "._default_log_printer")
_default_log_printer_logger.setLevel("INFO")
_default_log_printer_logger.propagate = False
_default_log_printer_handler = logging.StreamHandler()
_default_log_printer_handler.setFormatter(logging.Formatter(fmt="%(message)s"))
_default_log_printer_logger.addHandler(_default_log_printer_handler)

_TASK_ID_STACK_LOG_ANNOTATION_KEY = "task_id_stack"


_ARTIFACT_PATH_LAST_PART = "data"


def _default_log_printer(log_entry: launchers.ProcessLogEntry):
    log_text = log_entry.message_bytes.decode("utf-8").replace("\r\n", "\n").rstrip()
    if log_entry.annotations:
        task_id_stack = log_entry.annotations.get(_TASK_ID_STACK_LOG_ANNOTATION_KEY)
        if task_id_stack:
            log_text = "[" + "/".join(list(task_id_stack)) + "] " + log_text
    if log_entry.time:
        log_text = log_entry.time.isoformat(sep=" ") + ": " + log_text
    _default_log_printer_logger.info(log_text, extra=log_entry.annotations)


class Runner:
    def __init__(
        self,
        task_launcher: launchers.ContainerTaskLauncher,
        root_uri: storage_providers.UriAccessor,
        on_log_entry_callback: Optional[
            Callable[[launchers.ProcessLogEntry], None]
        ] = _default_log_printer,
    ):
        self._task_launcher = task_launcher
        self._root_uri = root_uri
        self._on_log_entry_callback = on_log_entry_callback
        self._futures_executor = futures.ThreadPoolExecutor()
        self._artifacts_root = root_uri.make_subpath(relative_path="artifacts")

    def _generate_artifact_data_uri(
        self,
    ) -> storage_providers.UriAccessor:
        return self._artifacts_root.make_subpath(
            relative_path="uuid=" + uuid.uuid4().hex
        ).make_subpath(relative_path="data")

    def _create_artifact_from_local_data(
        self, path: str, type_spec: Optional[_structures.TypeSpecType]
    ) -> "_StorageArtifact":
        uri_accessor = self._generate_artifact_data_uri()
        uri_accessor.get_writer().upload_from_path(path=path)
        artifact = _StorageArtifact(
            uri_reader=uri_accessor.get_reader(), type_spec=type_spec
        )
        return artifact

    def _create_artifact_from_object(
        self, obj: Any, type_spec: Optional[_structures.TypeSpecType]
    ) -> "_StorageArtifact":
        with tempfile.TemporaryDirectory() as temp_dir:
            # We could make the last path part anything, not just "data".
            data_path = os.path.join(temp_dir, _ARTIFACT_PATH_LAST_PART)
            type_spec = _serialization.save(
                obj=obj, path=data_path, type_spec=type_spec
            )
            return self._create_artifact_from_local_data(
                path=data_path, type_spec=type_spec
            )

    def _run_graph_task(
        self,
        task_spec: structures.TaskSpec,
        # graph_input_artifacts: Mapping[str, "_StorageArtifact"],
        graph_input_artifacts: Mapping[
            str,
            Union[
                "_StorageArtifact",
                "_FutureExecutionOutputArtifact",
            ],
        ],
        task_id_stack: List[str],
    ) -> "GraphExecution":
        task_id_to_output_artifacts_map: Dict[
            str, Dict[str, "_StorageArtifact"]
        ] = {}  # Task ID -> output name -> artifact

        component_ref: structures.ComponentReference = task_spec.component_ref
        component_spec: structures.ComponentSpec = component_ref.spec
        graph_spec: structures.GraphSpec = component_spec.implementation.graph
        toposorted_tasks = graph_spec._toposorted_tasks

        graph_output_artifacts: Dict[str, artifact_stores.Artifact] = {}
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
                artifact: Optional[Union[str, artifact_stores.Artifact]]
                if isinstance(argument, str):
                    # Not uploading the argument here. We'll do this it in _run_task.
                    # Artifact needs type and we do not know the input type yet since component is not loaded yet.
                    artifact = argument
                elif isinstance(argument, structures.GraphInputArgument):
                    # Graph inputs can have no arguments passed to them
                    artifact = graph_input_artifacts.get(
                        argument.graph_input.input_name
                    )
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
                if artifact is not None:
                    task_input_artifacts[input_name] = artifact

            child_task_execution = self._run_task(
                task_spec=child_task_spec,
                input_arguments=task_input_artifacts,
                task_id_stack=task_id_stack + [child_task_id],
            )
            task_executions[child_task_id] = child_task_execution

            task_id_to_output_artifacts_map[
                child_task_id
            ] = child_task_execution.outputs

        graph_execution._waiters = [
            task_execution.wait_for_completion
            for task_execution in graph_execution.task_executions.values()
        ]

        # Preparing graph outputs
        for output_spec in component_spec.outputs or []:
            output_name = output_spec.name
            output_source = graph_spec.output_values[
                output_name
            ]  # The entry must exist

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

    def run_component(
        self,
        component: Union[
            Callable,
            structures.ComponentSpec,
            structures.ComponentReference,
        ],
        arguments: Optional[Mapping[str, Union[str, artifact_stores.Artifact]]] = None,
        annotations: Optional[Dict[str, Any]] = None,
        task_name: Optional[str] = None,
    ):
        component_ref: structures.ComponentReference
        if isinstance(component, structures.ComponentReference):
            component_ref = component
        elif isinstance(component, structures.ComponentSpec):
            component_spec = component
            component_ref = structures.ComponentReference(spec=component_spec)
        else:
            maybe_component_ref = getattr(component, "_component_ref", None) or getattr(
                component, "component_ref", None
            )
            if maybe_component_ref:
                if isinstance(maybe_component_ref, structures.ComponentReference):
                    component_ref = maybe_component_ref
                else:
                    raise TypeError(
                        f"Unsupported component reference: {maybe_component_ref}"
                    )
            else:
                maybe_component_spec = getattr(component, "component_spec", None)
                if maybe_component_spec:
                    if isinstance(maybe_component_spec, structures.ComponentSpec):
                        component_ref = structures.ComponentReference(
                            spec=maybe_component_spec
                        )
                    else:
                        raise TypeError(
                            f"Unsupported component spec: {maybe_component_spec}"
                        )
                else:
                    raise TypeError(f"Could not find component in {component}")

        task_spec = structures.TaskSpec(
            component_ref=component_ref,
            annotations=annotations,
        )
        return self.run_task(
            task_spec=task_spec,
            input_arguments=arguments,
            task_name=task_name,
        )

    def run_task(
        self,
        task_spec: structures.TaskSpec,
        input_arguments: Optional[
            Mapping[str, Union[str, artifact_stores.Artifact]]
        ] = None,
        task_name: Optional[str] = None,
    ) -> "Execution":
        for argument in (task_spec.arguments or {}).values():
            if isinstance(argument, str):
                pass
            elif isinstance(argument, structures.GraphInputArgument):
                raise NotImplementedError
            elif isinstance(argument, structures.TaskOutputArgument):
                raise NotImplementedError
            else:
                raise TypeError(
                    "Unsupported argument type: {} - {}.".format(
                        str(type(argument).__name__), str(argument)
                    )
                )
        full_input_arguments = task_spec.arguments or {}
        if input_arguments:
            full_input_arguments.update(input_arguments)

        return self._run_task(
            task_spec=task_spec,
            input_arguments=full_input_arguments,
            task_id_stack=[task_name] if task_name else None,
        )

    def _run_task(
        self,
        task_spec: structures.TaskSpec,
        input_arguments: Mapping[
            str,
            Union[
                str,
                "_StorageArtifact",
                "_FutureExecutionOutputArtifact",
            ],
        ],
        task_id_stack: Optional[List[str]] = None,
    ) -> "Execution":
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

        input_specs = {
            input_spec.name: input_spec for input_spec in component_spec.inputs or []
        }

        # Important: We need to take care of the optional graph component inputs.
        # For container components we do not pass anything for missing arguments to optional inputs.
        # However for graph components we need to figure out how to make child components with required inputs receive the default.
        # For an *optional* *graph* component input, we need to set the missing argument to the input default *if it exists*.
        # optional+, default+, container => do not pass argument
        # optional+, default+, graph => use default (*special case*)
        # optional+, default- => do not pass argument
        # optional-, default+ => use default (for container components, resolve_command_line can handle this)
        # optional-, default- => error
        input_arguments = dict(input_arguments)
        for input_spec in component_spec.inputs or []:
            if input_spec.name not in input_arguments:
                if input_spec.optional:
                    if (
                        isinstance(
                            component_spec.implementation,
                            structures.GraphImplementation,
                        )
                        and input_spec.default is not None
                    ):
                        input_arguments[input_spec.name] = input_spec.default
                else:
                    if input_spec.default is None:
                        raise ValueError(
                            f'No argument provided for the required input "{input_spec.name}" with no default.'
                        )
                    else:
                        input_arguments[input_spec.name] = input_spec.default

        # TODO: Check artifact type compatibility. Can we share this between compilation and interactive?
        input_artifacts = {
            input_name: (
                argument
                if isinstance(argument, artifact_stores.Artifact)
                else self._create_artifact_from_object(
                    obj=argument, type_spec=input_specs[input_name].type
                )
            )
            for input_name, argument in input_arguments.items()
        }

        if not task_id_stack:
            task_id_stack = [component_spec.name or "Task"]

        if isinstance(
            component_spec.implementation, structures.ContainerImplementation
        ):
            output_names = [
                output_spec.name for output_spec in component_spec.outputs or []
            ]
            output_specs = {
                output_spec.name: output_spec
                for output_spec in component_spec.outputs or []
            }
            output_artifacts = {}

            start_time = datetime.datetime.utcnow()
            execution = ContainerExecution(
                start_time=start_time,
                task_spec=task_spec,
                input_arguments=input_artifacts,
                status=ExecutionStatus.WaitingForUpstream,
                outputs=output_artifacts,
            )

            for output_name in output_names:
                output_artifacts[output_name] = _FutureExecutionOutputArtifact(
                    execution=execution,
                    output_name=output_name,
                    type_spec=output_specs[output_name].type,
                )

            def add_task_ids_to_log_entries(log_entry: launchers.ProcessLogEntry):
                if task_id_stack:
                    if not log_entry.annotations:
                        log_entry.annotations = {}
                    log_entry.annotations[_TASK_ID_STACK_LOG_ANNOTATION_KEY] = list(
                        task_id_stack
                    )
                if self._on_log_entry_callback:
                    self._on_log_entry_callback(log_entry)

            on_log_entry_callback = (
                add_task_ids_to_log_entries if self._on_log_entry_callback else None
            )

            def log_message(message: str):
                if on_log_entry_callback:
                    log_entry = launchers.ProcessLogEntry(
                        message_bytes=message.encode("utf-8"),
                        time=datetime.datetime.utcnow(),
                    )
                    on_log_entry_callback(log_entry)

            def launch_container_task_and_set_output_artifact_futures():
                try:
                    # We might not need to resolve the artifacts explicitly before calling the task launcher,
                    # but this makes the error handling easier and avoid exposing the Execution class to the launchers.
                    try:
                        resolved_input_artifacts = {
                            input_name: (
                                argument._get_artifact()
                                if isinstance(argument, _FutureExecutionOutputArtifact)
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
                        exception = UpstreamExecutionFailedError(
                            execution=execution,
                            upstream_execution=failed_upstream_execution,
                        )
                        raise exception  # from e

                    input_uri_readers = {
                        input_name: artifact._uri_reader
                        for input_name, artifact in resolved_input_artifacts.items()
                    }
                    output_uris = {
                        output_name: self._generate_artifact_data_uri()
                        for output_name in output_names
                    }
                    output_uri_writers = {
                        output_name: uri_accessor.get_writer()
                        for output_name, uri_accessor in output_uris.items()
                    }

                    execution.status = ExecutionStatus.Starting
                    log_message(message="Starting container task.")
                    launched_container = self._task_launcher.launch_container_task(
                        task_spec=task_spec,
                        input_uri_readers=input_uri_readers,
                        output_uri_writers=output_uri_writers,
                    )
                    execution.status = ExecutionStatus.Running
                    execution._launched_container = launched_container
                    container_execution_result = launched_container.wait_for_completion(
                        on_log_entry_callback=on_log_entry_callback
                    )

                    execution.end_time = container_execution_result.end_time
                    execution.exit_code = container_execution_result.exit_code
                    execution.log = container_execution_result.log
                    if container_execution_result.exit_code == 0:
                        execution.status = ExecutionStatus.Succeeded

                        for output_name, output_uri in output_uris.items():
                            output_artifact = _StorageArtifact(
                                uri_reader=output_uri.get_reader(),
                                type_spec=output_specs[output_name].type,
                            )
                            output_artifacts[output_name] = output_artifact
                    else:
                        execution.status = ExecutionStatus.Failed
                    log_message(
                        message=f"Container task completed with status: {execution.status.name}"
                    )
                    if execution.status == ExecutionStatus.Failed:
                        raise ExecutionFailedError(execution=execution)
                    return container_execution_result
                # The status of Failed executions should not be changed to SystemError
                except ExecutionFailedError:
                    raise
                except Exception as ex:
                    execution.status = ExecutionStatus.SystemError
                    execution.end_time = datetime.datetime.utcnow()
                    execution._error_message = repr(ex)
                    raise ExecutionFailedError(execution=execution) from ex

            container_launch_future = self._futures_executor.submit(
                launch_container_task_and_set_output_artifact_futures
            )
            execution._waiters = [container_launch_future.result]
            # execution._container_launch_future = container_launch_future
            return execution
        elif isinstance(component_spec.implementation, structures.GraphImplementation):
            return self._run_graph_task(
                task_spec=task_spec,
                graph_input_artifacts=input_artifacts,
                task_id_stack=task_id_stack or [],
            )
        else:
            raise RuntimeError(
                f"Unsupported component implementation: {component_spec.implementation}"
            )


class InteractiveMode:
    def __init__(
        self,
        task_launcher: Optional[launchers.ContainerTaskLauncher] = None,
        root_uri: Optional[storage_providers.UriAccessor] = None,
        wait_for_completion_on_exit: bool = True,
    ):
        if not task_launcher:
            from .launchers.local_docker_launcher import DockerContainerLauncher

            task_launcher = DockerContainerLauncher()
        if not root_uri:
            from .storage_providers import local_storage
            import tempfile

            root_dir = tempfile.mkdtemp(prefix="cloud_pipelines.")

            root_uri = local_storage.LocalStorageProvider().make_uri(root_dir)
        self._runner = Runner(
            task_launcher=task_launcher,
            root_uri=root_uri,
        )
        self._old_container_task_constructor = None
        self._wait_for_completion_on_exit = wait_for_completion_on_exit
        self._executions: Dict[str, Execution] = {}

    def __enter__(self):
        def _create_execution_from_component_and_arguments(
            arguments: Mapping[str, Any],
            component_ref: structures.ComponentReference,
            **kwargs,
        ) -> Execution:
            task_spec = structures.TaskSpec(
                component_ref=component_ref,
            )
            task_id = component_ref.spec.name if component_ref.spec else "Task"
            task_id = _make_name_unique_by_adding_index(
                task_id, self._executions.keys(), " "
            )
            execution = self._runner.run_task(
                task_spec=task_spec,
                input_arguments=arguments,
                task_name=task_id,
            )
            self._executions[task_id] = execution
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
            for execution in self._executions.values():
                execution.wait_for_completion()

    _interactive_mode: Optional["InteractiveMode"] = None

    @staticmethod
    def activate(
        task_launcher: Optional[launchers.ContainerTaskLauncher] = None,
        root_uri: Optional[storage_providers.UriAccessor] = None,
        wait_for_completion_on_exit: bool = True,
    ):
        if InteractiveMode._interactive_mode:
            raise RuntimeError("Already in eager mode.")

        InteractiveMode._interactive_mode = InteractiveMode(
            task_launcher=task_launcher,
            root_uri=root_uri,
            wait_for_completion_on_exit=wait_for_completion_on_exit,
        )
        InteractiveMode._interactive_mode.__enter__()

    @staticmethod
    def deactivate():
        interactive_mode = InteractiveMode._interactive_mode
        InteractiveMode._interactive_mode = None
        if not interactive_mode:
            raise RuntimeError("Not in eager mode.")
        interactive_mode.__exit__(None, None, None)


activate_interactive_mode = InteractiveMode.activate
deactivate_interactive_mode = InteractiveMode.deactivate


def _make_name_unique_by_adding_index(
    name: str, collection: typing.Container[str], delimiter: str
):
    unique_name = name
    if unique_name in collection:
        for i in range(2, 100000):
            unique_name = name + delimiter + str(i)
            if unique_name not in collection:
                break
    return unique_name


class ExecutionStatus(enum.Enum):
    Invalid = 0
    WaitingForUpstream = 1
    Starting = 2
    Running = 3
    Succeeded = 4
    Failed = 5
    UpstreamFailed = 6
    ConditionallySkipped = 7
    SystemError = 8


@dataclasses.dataclass
class Execution:
    task_spec: structures.TaskSpec
    input_arguments: Mapping[str, artifact_stores.Artifact]
    outputs: Mapping[str, artifact_stores.Artifact]

    def wait_for_completion(self):
        for waiter in self._waiters or []:
            waiter()


@dataclasses.dataclass
class ContainerExecution(Execution):
    status: ExecutionStatus = ExecutionStatus.Invalid
    start_time: Optional[datetime.datetime] = None
    end_time: Optional[datetime.datetime] = None
    exit_code: Optional[int] = None
    log: Optional[launchers.ProcessLog] = None
    _waiters: Optional[Sequence[Callable[[], Any]]] = None
    # TODO: Launcher-specific info

    def __repr__(self):
        component_spec = self.task_spec.component_ref.spec
        component_name = component_spec.name or "component"
        return f"""<ContainerExecution(component="{component_name}", status="{self.status.name}")>"""


@dataclasses.dataclass
class GraphExecution(Execution):
    task_executions: Mapping[str, Execution]
    _waiters: Optional[Sequence[Callable[[], Any]]] = None


class ExecutionFailedError(Exception):
    def __init__(self, execution):
        self.execution = execution
        super().__init__(self, execution)


class UpstreamExecutionFailedError(Exception):
    def __init__(self, execution, upstream_execution):
        self.execution = execution
        self.upstream_execution = upstream_execution
        super().__init__(self, execution, upstream_execution)


class _StorageArtifact(artifact_stores.Artifact):
    def __init__(
        self,
        uri_reader: storage_providers.UriReader,
        type_spec: Optional[_structures.TypeSpecType] = None,
    ):
        super().__init__(type_spec=type_spec)
        self._uri_reader = uri_reader

    def _download_to_path(self, path: str):
        self._uri_reader.download_to_path(path=path)

    def _download_as_bytes(self) -> bytes:
        return self._uri_reader.download_as_bytes()


class _FutureExecutionOutputArtifact(artifact_stores.Artifact):
    def __init__(
        self,
        execution: Execution,
        output_name: str,
        type_spec: Optional[_structures.TypeSpecType] = None,
    ):
        super().__init__(type_spec=type_spec)
        self._execution = execution
        self._output_name = output_name

    def _get_artifact(self) -> _StorageArtifact:
        self._execution.wait_for_completion()
        artifact = self._execution.outputs[self._output_name]
        assert isinstance(artifact, _StorageArtifact)
        return artifact

    def _download_to_path(self, path: str):
        self._get_artifact()._download_to_path(path=path)

    def _download_as_bytes(self) -> bytes:
        return self._get_artifact()._download_as_bytes()
