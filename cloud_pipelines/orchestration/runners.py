from concurrent import futures
import dataclasses
import datetime
import enum
import hashlib
import json
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
from .storage_providers import local_storage

from ..components import _serialization


_default_log_printer_logger = logging.getLogger(__name__ + "._default_log_printer")
_default_log_printer_logger.setLevel("INFO")
_default_log_printer_logger.propagate = False
_default_log_printer_handler = logging.StreamHandler()
_default_log_printer_handler.setFormatter(logging.Formatter(fmt="%(message)s"))
_default_log_printer_logger.addHandler(_default_log_printer_handler)

_TASK_ID_STACK_LOG_ANNOTATION_KEY = "task_id_stack"


_ARTIFACT_PATH_LAST_PART = "data"

_ARTIFACT_DATA_HASH = "md5"


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
        self._artifact_data_dir = root_uri.make_subpath(relative_path="artifact_data")
        self._db_dir = root_uri.make_subpath(relative_path="db")
        self._executions_table_dir = self._db_dir.make_subpath(
            relative_path="container_executions"
        )
        self._cached_execution_ids_table_dir = self._db_dir.make_subpath(
            relative_path="container_executions_cache"
        )
        self._execution_cache = _ExecutionCacheDb(
            cached_execution_ids_table_dir=self._cached_execution_ids_table_dir,
            executions_table_dir=self._executions_table_dir,
        )
        self._artifact_data_info_table_dir = self._db_dir.make_subpath(
            relative_path="artifact_data_info"
        )

    def _generate_artifact_data_uri(
        self,
    ) -> storage_providers.UriAccessor:
        return self._artifact_data_dir.make_subpath(
            relative_path="uuid=" + uuid.uuid4().hex
        ).make_subpath(relative_path="data")

    def _create_artifact_from_local_data(
        self, path: str, type_spec: Optional[_structures.TypeSpecType]
    ) -> "_StorageArtifact":
        data_info = local_storage._get_data_info_from_path(path=path)
        data_hash = data_info.hashes[_ARTIFACT_DATA_HASH]
        data_key = f"{_ARTIFACT_DATA_HASH}={data_hash}"
        uri_accessor = self._artifact_data_dir.make_subpath(
            relative_path=data_key
        ).make_subpath(relative_path="data")
        # TODO: Detect the existence by querying the artifact DB, not the data storage.
        if not uri_accessor.get_reader().exists():
            # TODO: Make uploading reliable. Upload to temporary location then rename.
            uri_accessor.get_writer().upload_from_path(path=path)
        else:
            # Returning the artifact object that points to existing data
            # We expect the DB to not be in corrupted state.
            # We expect that the existing data with same hash is the same as the new data
            pass

        artifact = _StorageArtifact(
            uri_reader=uri_accessor.get_reader(), type_spec=type_spec
        )
        artifact._data_info = data_info
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

    def _deduplicate_storage_artifact(
        self, artifact: "_StorageArtifact"
    ) -> "_StorageArtifact":
        data_info = (
            getattr(artifact, "_data_info", None) or artifact._uri_reader.get_info()
        )
        data_hash = data_info.hashes[_ARTIFACT_DATA_HASH]
        data_key = f"{_ARTIFACT_DATA_HASH}={data_hash}"
        artifact_data_info_uri = self._artifact_data_info_table_dir.make_subpath(
            relative_path=data_key
        )
        if artifact_data_info_uri.get_reader().exists():
            # Returning the artifact object that points to existing data
            # We expect the DB to not be in corrupted state.
            # We expect that the existing data with same hash is the same as the new data

            artifact_data_info_struct = json.loads(
                artifact_data_info_uri.get_reader().download_as_text()
            )
            artifact_data_info_struct["type_spec"] = artifact._type_spec
            existing_data_artifact = _StorageArtifact._from_dict(
                artifact_data_info_struct, provider=self._root_uri._provider
            )
            # TODO: ! Delete the new artifact data
            assert existing_data_artifact._data_info == data_info
            return existing_data_artifact
        else:
            # TODO: Rename the artifact to directory based on the data hash.
            artifact._data_info = data_info
            artifact_data_info_struct = artifact._to_dict()
            # TODO: Create ArtifactData class that holds the artifact URI and info, but not type or execution.
            del artifact_data_info_struct["type_spec"]

            artifact_data_info_str = json.dumps(artifact_data_info_struct, indent=2)
            artifact_data_info_uri.get_writer().upload_from_text(artifact_data_info_str)
            return artifact

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

            execution_id = (
                start_time.strftime("%Y-%m-%d_%H-%M-%S_%f") + "_" + uuid.uuid4().hex
            )

            for output_name in output_names:
                output_artifacts[output_name] = _FutureExecutionOutputArtifact(
                    execution=execution,
                    output_name=output_name,
                    type_spec=output_specs[output_name].type,
                )

            execution_uri = self._executions_table_dir.make_subpath(execution_id)

            def write_execution_to_db():
                execution_struct = execution._to_dict()
                execution_string = json.dumps(execution_struct, indent=2)
                # # Verifying that the serialized execution can be loaded again
                # reloaded_execution_struct = json.loads(execution_string)
                # reloaded_execution = ContainerExecution._from_dict(
                #     reloaded_execution_struct,
                #     storage_provider=self._root_uri._provider,
                # )
                # execution.__dict__.update(reloaded_execution.__dict__)
                # Storing the execution itself
                execution_uri.get_writer().upload_from_text(execution_string)

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
                    start_time = datetime.datetime.utcnow()
                    execution.start_time = start_time

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

                    # Getting max_cached_data_staleness
                    max_cached_data_staleness_str = None
                    if (
                        task_spec.execution_options
                        and task_spec.execution_options.caching_strategy
                    ):
                        max_cached_data_staleness_str = (
                            task_spec.execution_options.caching_strategy.max_cache_staleness
                        )

                    execution_cache_key = self._execution_cache.get_execution_cache_key(
                        execution
                    )
                    cached_execution_with_id = (
                        self._execution_cache.try_get_execution_by_key(
                            execution_cache_key, max_cached_data_staleness_str
                        )
                    )

                    if cached_execution_with_id:
                        execution.__dict__ = cached_execution_with_id.execution.__dict__
                        execution._execution_id = cached_execution_with_id.id
                        # TODO: Add information to the ExecutionNode to indicate that the execution was reused from cache
                        log_bytes = execution._log_artifact._download_as_bytes()
                        execution.log = launchers.ProcessLog()
                        execution.log.add_entry(
                            launchers.ProcessLogEntry(message_bytes=log_bytes)
                        )
                        execution.log.close()
                        if on_log_entry_callback and len(log_bytes) > 0:
                            # TODO: Add a way to suppress the reused execution log
                            log_message(message="Reused execution log:")
                            log_text = log_bytes.decode("utf-8")
                            for log_line in log_text.split("\n"):
                                log_message(message=log_line)
                        log_message(message="Reused the execution from cache.")
                        return None  # Cached

                    # Preparing the log artifact
                    # TODO: Support JSONL log with timestamps
                    # log_uri = self._generate_artifact_data_uri().make_subpath("log.txt")
                    log_uri = self._generate_artifact_data_uri()
                    # Should log be an artifact or a separate entity?
                    execution._log_artifact = _StorageArtifact(
                        uri_reader=log_uri.get_reader(),
                        # TODO: Support materializing the log artifact
                        type_spec="Text log",
                    )

                    execution.status = ExecutionStatus.Starting
                    log_message(message="Starting container task.")
                    write_execution_to_db()
                    launched_container = self._task_launcher.launch_container_task(
                        task_spec=task_spec,
                        input_uri_readers=input_uri_readers,
                        output_uri_writers=output_uri_writers,
                        log_uri_writer=log_uri.get_writer(),
                    )
                    execution.status = ExecutionStatus.Running
                    execution._launched_container = launched_container
                    write_execution_to_db()
                    container_execution_result = launched_container.wait_for_completion(
                        on_log_entry_callback=on_log_entry_callback
                    )

                    execution.end_time = container_execution_result.end_time
                    execution.exit_code = container_execution_result.exit_code
                    execution.log = container_execution_result.log
                    if container_execution_result.exit_code == 0:
                        execution.status = ExecutionStatus.Succeeded

                        for output_name, output_uri in output_uris.items():
                            artifact_info = output_uri.get_reader().get_info()
                            output_artifact = _StorageArtifact(
                                uri_reader=output_uri.get_reader(),
                                type_spec=output_specs[output_name].type,
                            )
                            output_artifact._data_info = artifact_info
                            output_artifact = self._deduplicate_storage_artifact(
                                artifact=output_artifact
                            )
                            output_artifacts[output_name] = output_artifact
                    else:
                        execution.status = ExecutionStatus.Failed
                    log_message(
                        message=f"Container task completed with status: {execution.status.name}"
                    )
                    # Storing the execution in the db
                    write_execution_to_db()
                    # Storing successful execution in the execution cache
                    if execution.status == ExecutionStatus.Succeeded:
                        self._execution_cache.put_execution_id_in_cache(
                            execution_id=execution_id,
                            execution_cache_key=execution_cache_key,
                            max_cached_data_staleness_str=max_cached_data_staleness_str,
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
                    write_execution_to_db()
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
    # TODO: Integrate the `log` with `_log_artifact`
    log: Optional[launchers.ProcessLog] = None
    _log_artifact: Optional[artifact_stores.Artifact] = None
    _waiters: Optional[Sequence[Callable[[], Any]]] = None
    # TODO: Launcher-specific info

    def __repr__(self):
        component_spec = self.task_spec.component_ref.spec
        component_name = component_spec.name or "component"
        return f"""<ContainerExecution(component="{component_name}", status="{self.status.name}")>"""

    def _to_dict(self) -> dict:
        input_arguments_struct = {
            input_name: _assert_type(artifact, _StorageArtifact)._to_dict()
            for input_name, artifact in self.input_arguments.items()
        }
        outputs_struct = {}
        for output_name, artifact in self.outputs.items():
            # Skipping artifacts that are not produced yet
            if isinstance(artifact, _StorageArtifact):
                outputs_struct[output_name] = artifact._to_dict()
        result = {
            # Execution
            "task_spec": self.task_spec.to_dict(),
            "input_artifacts": input_arguments_struct,
            "output_artifacts": outputs_struct,
            # ContainerExecution
            "status": self.status.name,
            "start_time": self.start_time.isoformat(sep=" ")
            if self.start_time
            else None,
            "end_time": self.end_time.isoformat(sep=" ") if self.end_time else None,
            "exit_code": self.exit_code,
            "log_artifact": _assert_type(
                self._log_artifact, _StorageArtifact
            )._to_dict()
            if self._log_artifact
            else None,
        }
        return result

    def _to_cache_key_dict(self) -> dict:
        input_artifact_uri_structs = {
            input_name: _assert_type(
                artifact, _StorageArtifact
            )._uri_reader.uri.to_dict()
            for input_name, artifact in self.input_arguments.items()
        }
        component_struct = self.task_spec.component_ref.spec.to_dict()
        result = {
            # Execution
            "component_spec": component_struct,
            "input_artifact_uri_structs": input_artifact_uri_structs,
        }
        return result

    @staticmethod
    def _from_dict(
        dict: dict, storage_provider: storage_providers.StorageProvider
    ) -> "ContainerExecution":
        return ContainerExecution(
            task_spec=structures.TaskSpec.from_dict(dict["task_spec"]),
            input_arguments={
                input_name: _StorageArtifact._from_dict(
                    dict=artifact_struct, provider=storage_provider
                )
                for input_name, artifact_struct in dict["input_artifacts"].items()
            },
            outputs={
                output_name: _StorageArtifact._from_dict(
                    dict=artifact_struct, provider=storage_provider
                )
                for output_name, artifact_struct in dict["output_artifacts"].items()
            },
            # status=ExecutionStatus(dict["status"]),
            status=ExecutionStatus[dict["status"]],
            start_time=(
                datetime.datetime.fromisoformat(dict["start_time"])
                if dict["start_time"]
                else None
            ),
            end_time=(
                datetime.datetime.fromisoformat(dict["end_time"])
                if dict["end_time"]
                else None
            ),
            exit_code=dict["exit_code"],
            _log_artifact=(
                _StorageArtifact._from_dict(
                    dict=dict["log_artifact"], provider=storage_provider
                )
                if dict["log_artifact"]
                else None
            ),
        )


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

    def _get_info(self) -> storage_providers.interfaces.DataInfo:
        return self._uri_reader.get_info()

    def _to_dict(self) -> dict:
        data_uri_dict = self._uri_reader.uri.to_dict()
        result = {
            # TODO: Create a TypeSpec class that represents type_spec and has .to_dict()
            "type_spec": self._type_spec,
            "data_uri": data_uri_dict,
        }
        data_info = getattr(self, "_data_info", None)
        if data_info:
            result["data_info"] = dataclasses.asdict(data_info)
        return result

    @staticmethod
    def _from_dict(
        dict: dict, provider: storage_providers.StorageProvider
    ) -> "_StorageArtifact":
        data_uri = storage_providers.DataUri.from_dict(dict["data_uri"])
        uri_accessor = storage_providers.UriAccessor(uri=data_uri, provider=provider)
        type_spec = dict["type_spec"]
        storage_artifact = _StorageArtifact(
            uri_reader=uri_accessor.get_reader(), type_spec=type_spec
        )
        if "data_info" in dict:
            storage_artifact._data_info = storage_providers.DataInfo(
                **dict["data_info"]
            )
        return storage_artifact


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


_T = typing.TypeVar("_T")


def _assert_type(value: typing.Any, typ: typing.Type[_T]) -> _T:
    if not isinstance(value, typ):
        raise TypeError(f"Expected type {typ}, but got {type(value)}: {value}")
    return value


class _ExecutionWithId(typing.NamedTuple):
    id: str
    execution: ContainerExecution


class _ExecutionCacheDb:
    OLDEST_EXECUTION_CACHE_SUB_KEY = "oldest"
    LATEST_EXECUTION_CACHE_SUB_KEY = "latest"

    def __init__(
        self,
        cached_execution_ids_table_dir: storage_providers.UriAccessor,
        executions_table_dir: storage_providers.UriAccessor,
    ):
        self._executions_table_dir = executions_table_dir
        self._cached_execution_ids_table_dir = cached_execution_ids_table_dir

    def _try_load_execution_from_cache_by_key_and_tag(
        self,
        execution_cache_key: str,
        tag: str,
    ) -> Optional[_ExecutionWithId]:
        # TODO: Validate or sanitize cache_sub_key before using it as part of URI
        execution_id_uri = self._cached_execution_ids_table_dir.make_subpath(
            execution_cache_key
        ).make_subpath(tag)
        if not execution_id_uri.get_reader().exists():
            return None
        execution_id = execution_id_uri.get_reader().download_as_text()
        execution_uri = self._executions_table_dir.make_subpath(execution_id)
        execution_data = execution_uri.get_reader().download_as_text()
        execution_struct = json.loads(execution_data)
        loaded_execution = ContainerExecution._from_dict(
            execution_struct,
            storage_provider=self._executions_table_dir._provider,
        )
        return _ExecutionWithId(id=execution_id, execution=loaded_execution)

    @staticmethod
    def get_execution_cache_key(
        execution: ContainerExecution,
    ):
        execution_cache_key_struct = execution._to_cache_key_dict()
        execution_cache_key_struct_string = json.dumps(
            execution_cache_key_struct, sort_keys=True
        )
        execution_cache_key = (
            "sha256="
            + hashlib.sha256(
                execution_cache_key_struct_string.encode("utf-8")
            ).hexdigest()
        )
        return execution_cache_key

    def try_get_execution_by_key(
        self,
        execution_cache_key: str,
        max_cached_data_staleness_str: Optional[str],
    ) -> Optional[_ExecutionWithId]:
        # The cache reuse algorithm is pretty simple
        # if max_cache_staleness:
        #     Try reuse the execution with same max_cache_staleness (if viable) out of possibly many viable executions. This is a non-trivial part of the design.
        #     Else try reuse the "latest" execution (if viable) and set max_cache_staleness pointer to this execution
        #     Else create new execution and set max_cache_staleness pointer to this execution
        # if no max_cache_staleness:
        #     Try reuse the "oldest" execution (if exists)
        #     Else create new execution
        # If new execution was created and succeeded:
        #     If the "oldest" pointer is unset, then set it to the new execution
        #     Set the "latest" pointer to the new execution

        if max_cached_data_staleness_str:
            # First try to reuse execution with same max_cached_data_staleness.
            # This stabilizes which executions are reused.
            # If we reused the oldest execution in the max_cached_data_staleness time range,
            # then this execution will change every day since the time range window moves.
            # If we reused the latest execution,
            # then this execution will change every day since new executions can be created
            # (e.g. when some task has smaller max_cached_data_staleness time range).
            import isodate

            max_cached_data_staleness = isodate.parse_duration(
                max_cached_data_staleness_str
            )
            current_time = datetime.datetime.utcnow()

            period_cached_execution_with_id = (
                self._try_load_execution_from_cache_by_key_and_tag(
                    execution_cache_key=execution_cache_key,
                    tag=max_cached_data_staleness_str,
                )
            )
            if period_cached_execution_with_id and (
                period_cached_execution_with_id.execution.end_time
                + max_cached_data_staleness
                >= current_time
            ):
                # Reuse period_cached_execution
                return period_cached_execution_with_id
            # If we cannot reuse the period-based execution, let's try the latest execution.
            # If the execution satisfies the max_cached_data_staleness condition, then we set the period-based execution pointer to it.
            latest_cached_execution_with_id = (
                self._try_load_execution_from_cache_by_key_and_tag(
                    execution_cache_key=execution_cache_key,
                    tag=_ExecutionCacheDb.LATEST_EXECUTION_CACHE_SUB_KEY,
                )
            )
            if latest_cached_execution_with_id and (
                latest_cached_execution_with_id.execution.end_time
                + max_cached_data_staleness
                >= current_time
            ):
                # Setting the period-based execution pointer to the latest execution that we reuse.
                self._put_execution_id_in_cache_with_tag(
                    execution_id=latest_cached_execution_with_id.id,
                    execution_cache_key=execution_cache_key,
                    tag=max_cached_data_staleness_str,
                )
                # Reuse latest_cached_execution
                return latest_cached_execution_with_id
            # Could not find a suitable execution in the cache.
            return None
        else:
            # Trying to use the oldest execution.
            # This execution is "canonical" for the execution_cache_key and has the most cached downstream executions.
            oldest_cached_execution_with_id = (
                self._try_load_execution_from_cache_by_key_and_tag(
                    execution_cache_key=execution_cache_key,
                    tag=_ExecutionCacheDb.OLDEST_EXECUTION_CACHE_SUB_KEY,
                )
            )
            return oldest_cached_execution_with_id

    def _put_execution_id_in_cache_with_tag(
        self,
        execution_id: str,
        execution_cache_key: str,
        tag: str,
        overwrite: bool = True,
    ):
        cached_execution_id_uri = self._cached_execution_ids_table_dir.make_subpath(
            execution_cache_key
        ).make_subpath(tag)
        if overwrite or not cached_execution_id_uri.get_reader().exists():
            cached_execution_id_uri.get_writer().upload_from_text(execution_id)

    def put_execution_id_in_cache(
        self,
        execution_id: str,
        execution_cache_key: str,
        max_cached_data_staleness_str: Optional[str],
    ):
        # Storing the references to the execution in the execution cache.
        # Setting the oldest execution only the first time.
        self._put_execution_id_in_cache_with_tag(
            execution_id=execution_id,
            execution_cache_key=execution_cache_key,
            tag=_ExecutionCacheDb.OLDEST_EXECUTION_CACHE_SUB_KEY,
            overwrite=False,
        )
        # Always updating the latest execution
        self._put_execution_id_in_cache_with_tag(
            execution_id=execution_id,
            execution_cache_key=execution_cache_key,
            tag=_ExecutionCacheDb.LATEST_EXECUTION_CACHE_SUB_KEY,
        )
        # Updating the period-based cache
        if max_cached_data_staleness_str:
            self._put_execution_id_in_cache_with_tag(
                execution_id=execution_id,
                execution_cache_key=execution_cache_key,
                tag=max_cached_data_staleness_str,
            )
