import datetime
import os
import tempfile
import time
import typing
import unittest
from unittest import mock

# from ..components import create_component_from_func, create_graph_component_from_pipeline_func
from cloud_pipelines import components
from cloud_pipelines.components import structures


from cloud_pipelines.orchestration import runners

# from .launchers.local_environment_launcher import LocalEnvironmentContainerLauncher
# from .launchers.local_docker_launcher import DockerContainerLauncher
# from .launchers.local_kubernetes_launcher import LocalKubernetesContainerLauncher
from cloud_pipelines.orchestration.launchers import local_environment_launcher
from cloud_pipelines.orchestration.launchers.local_environment_launcher import (
    LocalEnvironmentLauncher,
)
from cloud_pipelines.orchestration.launchers.local_docker_launcher import (
    DockerContainerLauncher,
)
from cloud_pipelines.orchestration.launchers.local_kubernetes_launcher import (
    LocalKubernetesContainerLauncher,
)

from cloud_pipelines.orchestration.storage_providers import local_storage


_GOOGLE_CLOUD_STORAGE_ROOT_URI_ENV_KEY = "CLOUD_PIPELINES_GOOGLE_CLOUD_STORAGE_ROOT_URI"
_GOOGLE_CLOUD_STORAGE_ROOT_URI = os.environ.get(_GOOGLE_CLOUD_STORAGE_ROOT_URI_ENV_KEY)


def _build_data_passing_graph_component():
    # Components - Produce
    @components.create_component_from_func
    def produce_value() -> str:
        print("Component: produce_value")
        return "produce_value"

    @components.create_component_from_func
    def produce_file(data_path: components.OutputPath()):
        print("Component: produce_file")
        with open(data_path, "w") as f:
            f.write("produce_file")

    @components.create_component_from_func
    def produce_dir(data_path: components.OutputPath()):
        import pathlib

        print("Component: produce_dir")
        output_dir_path_obj = pathlib.Path(data_path)
        output_dir_path_obj.mkdir(parents=True, exist_ok=True)
        for i in range(10):
            (output_dir_path_obj / f"file_{i}.txt").write_text(str(i))

    # Components - Consume
    @components.create_component_from_func
    def consume_as_value(data):
        print("Component: consume_as_value: " + data)

    @components.create_component_from_func
    def consume_as_file(data_path: components.InputPath()):
        with open(data_path) as f:
            print("Component: consume_as_file: " + f.read())

    @components.create_component_from_func
    def consume_as_dir(data_path: components.InputPath()):
        import os

        print("Component: consume_as_dir")
        print(os.listdir(path=data_path))

    # Pipeline
    def pipeline3_func(
        graph_input_1: str = "graph_input_1_default",
    ):
        data1 = produce_value().output
        data2 = produce_file().output

        consume_as_value(data="constant_string")
        consume_as_value(data=graph_input_1)
        consume_as_value(data=data1)
        consume_as_value(data=data2)

        consume_as_file(data="constant_string")
        consume_as_file(data=graph_input_1)
        consume_as_file(data=data1)
        consume_as_file(data=data2)

        # Directories
        dir1 = produce_dir().output
        consume_as_dir(data=dir1)

        return dict(
            output_1=data1,
            output_2=data2,
            output_3=dir1,
        )

    return components.create_graph_component_from_pipeline_func(pipeline3_func)


def _build_nested_graph_component():
    pipeline_op = _build_data_passing_graph_component()

    def nested_pipeline(outer_graph_input_1: str = "outer_graph_input_1_default"):
        p1_task = pipeline_op(graph_input_1=outer_graph_input_1)
        p2_task = pipeline_op(graph_input_1=p1_task.outputs["output_1"])

        return dict(
            output_1=p2_task.outputs["output_1"],
        )

    nested_pipeline_op = components.create_graph_component_from_pipeline_func(
        nested_pipeline
    )
    return nested_pipeline_op


@components.create_component_from_func
def produce_value() -> str:
    print("Component: produce_value")
    return "produce_value"


@components.create_component_from_func
def consume_as_value(data):
    print("Component: consume_as_value: " + data)


@components.create_component_from_func
def fail():
    import sys

    print("Failing...")
    sys.exit(42)


@components.create_component_from_func
def generate_random_number(dummy: int = None) -> int:
    import random

    return random.randint(0, 1000000)


@components.create_component_from_func
def wait_then_return_random(dummy: int = 42, wait_seconds: int = 1) -> int:
    import random
    import time

    time.sleep(wait_seconds)
    return random.randint(0, 100000000)


@components.create_component_from_func
def _produce_and_consume_component(
    output_model_path: components.OutputPath("Model"),
    input_dataset_path: components.InputPath("Dataset") = None,
    input_string: str = "default string",
    input_integer: int = 42,
    input_float: float = 3.14,
    input_boolean: bool = True,
    input_list: list = ["s", 42, 3.14, [{}], {"k": []}],
    input_dict: dict = {
        "str": "s",
        "int": 42,
        "float": 3.14,
        "list": [{}],
        "dict": {"k": []},
    },
) -> typing.NamedTuple(
    "Outputs",
    [
        ("output_string", str),
        ("output_integer", int),
        ("output_float", float),
        ("output_boolean", bool),
        ("output_list", list),
        ("output_dict", dict),
    ],
):
    import pathlib

    if input_dataset_path:
        file_text = pathlib.Path(input_dataset_path).read_text()
    else:
        file_text = "no file passed"
    pathlib.Path(output_model_path).write_text(file_text)
    return (
        input_string,
        input_integer,
        input_float,
        input_boolean,
        input_list,
        input_dict,
    )


@components.create_component_from_func
def produce_string(
    string: str = "string",
    dummy: str = "dummy",
) -> str:
    return string


@components.create_component_from_func
def produce_integer(
    integer: int = 42,
    dummy: int = 0,
) -> int:
    return integer


@components.create_component_from_func
def produce_two_equal_strings(
    string: str = "string",
) -> typing.NamedTuple("Outputs", [("string1", str), ("string2", str),]):
    return (string, string)


@components.create_component_from_func
def write_text_to_stdout_and_stderr(text="Hello world"):
    import sys

    print("stdout:" + text, file=sys.stdout)
    print("stderr:" + text, file=sys.stderr)


class LaunchersTestCase(unittest.TestCase):
    def test_local_environment_launcher(self):
        nested_pipeline_op = _build_nested_graph_component()
        pipeline_task = nested_pipeline_op(outer_graph_input_1="outer_graph_input_1")

        with tempfile.TemporaryDirectory() as output_dir:
            runner = runners.Runner(
                task_launcher=LocalEnvironmentLauncher(),
                root_uri=local_storage.LocalStorageProvider().make_uri(path=output_dir),
            )
            execution = runner.run_task(
                task_spec=pipeline_task,
            )
            execution.wait_for_completion()

            # Test logs
            execution2 = runner.run_component(write_text_to_stdout_and_stderr)
            execution2.wait_for_completion()
            log_text = execution2._log_reader.download_as_text()
            assert "stdout:Hello world" in log_text
            assert "stderr:Hello world" in log_text

    def test_local_docker_launcher(self):
        component = _build_data_passing_graph_component()

        with tempfile.TemporaryDirectory() as output_dir:
            runner = runners.Runner(
                task_launcher=DockerContainerLauncher(),
                root_uri=local_storage.LocalStorageProvider().make_uri(path=output_dir),
            )
            execution = runner.run_component(
                component=component,
                arguments=dict(graph_input_1="graph_input_1"),
            )
            execution.wait_for_completion()

            # Test logs
            execution2 = runner.run_component(write_text_to_stdout_and_stderr)
            execution2.wait_for_completion()
            log_text = execution2._log_reader.download_as_text()
            assert "stdout:Hello world" in log_text
            assert "stderr:Hello world" in log_text

    def test_local_kubernetes_launcher(self):
        component = _build_data_passing_graph_component()

        with tempfile.TemporaryDirectory() as output_dir:
            runner = runners.Runner(
                task_launcher=LocalKubernetesContainerLauncher(),
                root_uri=local_storage.LocalStorageProvider().make_uri(path=output_dir),
            )
            execution = runner.run_component(
                component=component,
                arguments=dict(graph_input_1="graph_input_1"),
            )
            execution.wait_for_completion()

            # Test logs
            execution2 = runner.run_component(write_text_to_stdout_and_stderr)
            execution2.wait_for_completion()
            log_text = execution2._log_reader.download_as_text()
            assert "stdout:Hello world" in log_text
            assert "stderr:Hello world" in log_text

    @unittest.skipIf(
        condition=_GOOGLE_CLOUD_STORAGE_ROOT_URI is None,
        reason="Root GCS URI is not set",
    )
    def test_google_cloud_batch_launcher(self):
        if not _GOOGLE_CLOUD_STORAGE_ROOT_URI:
            self.skipTest(reason="Root GCS URI is not set")
            return

        from cloud_pipelines.orchestration.launchers.google_cloud_batch_launcher import (
            GoogleCloudBatchLauncher,
        )
        from cloud_pipelines.orchestration.storage_providers.google_cloud_storage import (
            GoogleCloudStorageProvider,
        )

        component = _build_data_passing_graph_component()

        runner = runners.Runner(
            task_launcher=GoogleCloudBatchLauncher(),
            root_uri=GoogleCloudStorageProvider().make_uri(
                uri=_GOOGLE_CLOUD_STORAGE_ROOT_URI
            ),
        )
        execution = runner.run_component(
            component=component,
        )
        execution.wait_for_completion()

    def test_interactive_mode_activate(self):
        # Disabling the interactive mode on exception to not affect other tests
        try:
            runners.InteractiveMode.activate()

            data1 = produce_value().outputs["Output"]
            consume_as_value(data=data1)
            consume_as_value(data="constant_value")
        finally:
            runners.InteractiveMode.deactivate()

    def test_interactive_mode_context(self):
        with runners.InteractiveMode():
            data1 = produce_value().outputs["Output"]
            consume_as_value(data=data1)
            consume_as_value(data="constant_value")

    def test_runner_run_component_with_callable(self):
        with tempfile.TemporaryDirectory() as output_dir:
            runner = runners.Runner(
                task_launcher=LocalEnvironmentLauncher(),
                root_uri=local_storage.LocalStorageProvider().make_uri(path=output_dir),
            )
            execution1 = runner.run_component(
                component=consume_as_value,
                arguments={"data": "test_runner_run_component"},
                annotations={"annotation_key": "annotation_value"},
            )
            execution1.wait_for_completion()
            self.assertIsInstance(execution1, runners.ContainerExecution)
            assert isinstance(execution1, runners.ContainerExecution)
            self.assertEqual(execution1.status, runners.ExecutionStatus.Succeeded)

    def test_execution_has_end_time_and_exit_code_when_failed(self):
        with tempfile.TemporaryDirectory() as output_dir:
            runner = runners.Runner(
                task_launcher=LocalEnvironmentLauncher(),
                root_uri=local_storage.LocalStorageProvider().make_uri(path=output_dir),
            )
            execution = runner.run_component(
                component=fail,
            )
            with self.assertRaises(runners.ExecutionFailedError):
                execution.wait_for_completion()
        assert isinstance(execution, runners.ContainerExecution)
        self.assertEqual(execution.status, runners.ExecutionStatus.Failed)
        self.assertIsNotNone(execution.start_time)
        self.assertIsNotNone(execution.end_time)
        self.assertEqual(execution.exit_code, 42)

    def test_runner_handles_serialization_of_basic_types(self):
        with runners.InteractiveMode():
            # Maybe I should actually validate the received values.
            # But the Lightweight Python Components tests already check this.
            execution = _produce_and_consume_component(
                input_string="input_string",
                input_integer=37,
                input_float=2.73,
                input_boolean=False,
                input_list=["item"],
                input_dict={"key1": "value1"},
            )
        assert isinstance(execution, runners.ContainerExecution)
        self.assertEqual(execution.status, runners.ExecutionStatus.Succeeded)
        self.assertIsNone(getattr(execution, "_error_message", None))

    def test_runner_raises_exception_when_it_cannot_serialize_arguments(self):
        with runners.InteractiveMode():
            with self.assertRaises(ValueError):
                _produce_and_consume_component(
                    input_string=42,
                )
            with self.assertRaises(TypeError):
                consume_as_value(
                    data=LaunchersTestCase,
                )

    def test_execution_input_artifacts_have_types(self):
        with runners.InteractiveMode():
            execution: runners.ContainerExecution = _produce_and_consume_component(
                input_string="input_string",
                input_integer=37,
                input_float=2.73,
                input_boolean=False,
                input_list=["item"],
                input_dict={"key1": "value1"},
            )
            # Checking the input artifact types
            input_arguments = execution.input_arguments
            self.assertEqual(input_arguments["input_string"]._type_spec, "String")
            self.assertEqual(input_arguments["input_integer"]._type_spec, "Integer")
            self.assertEqual(input_arguments["input_float"]._type_spec, "Float")
            self.assertEqual(input_arguments["input_boolean"]._type_spec, "Boolean")
            self.assertEqual(input_arguments["input_list"]._type_spec, "JsonArray")
            self.assertEqual(input_arguments["input_dict"]._type_spec, "JsonObject")

    def test_execution_output_artifacts_have_types(self):
        with runners.InteractiveMode():
            execution: runners.ContainerExecution = _produce_and_consume_component()
            # Checking the output artifact types
            self.assertEqual(execution.outputs["output_string"]._type_spec, "String")
            self.assertEqual(execution.outputs["output_integer"]._type_spec, "Integer")
            self.assertEqual(execution.outputs["output_float"]._type_spec, "Float")
            self.assertEqual(execution.outputs["output_boolean"]._type_spec, "Boolean")
            self.assertEqual(execution.outputs["output_list"]._type_spec, "JsonArray")
            self.assertEqual(execution.outputs["output_dict"]._type_spec, "JsonObject")
            execution.wait_for_completion()
            # Checking again since the future-based execution output artifacts are replaced after success.
            self.assertEqual(execution.outputs["output_string"]._type_spec, "String")
            self.assertEqual(execution.outputs["output_integer"]._type_spec, "Integer")
            self.assertEqual(execution.outputs["output_float"]._type_spec, "Float")
            self.assertEqual(execution.outputs["output_boolean"]._type_spec, "Boolean")
            self.assertEqual(execution.outputs["output_list"]._type_spec, "JsonArray")
            self.assertEqual(execution.outputs["output_dict"]._type_spec, "JsonObject")

    def test_artifact_materialization(self):
        with runners.InteractiveMode():
            execution: runners.ContainerExecution = _produce_and_consume_component(
                input_string="string",
                input_integer=37,
                input_float=2.73,
                input_boolean=False,
                input_list=["a", "b"],
                input_dict={"k": "v"},
            )
            self.assertEqual(execution.outputs["output_string"].materialize(), "string")
            self.assertEqual(execution.outputs["output_integer"].materialize(), 37)
            self.assertEqual(execution.outputs["output_float"].materialize(), 2.73)
            self.assertEqual(execution.outputs["output_boolean"].materialize(), False)
            self.assertEqual(execution.outputs["output_list"].materialize(), ["a", "b"])
            self.assertEqual(execution.outputs["output_dict"].materialize(), {"k": "v"})
            with self.assertRaises(ValueError):
                execution.outputs["output_model"].materialize()

    def test_reusing_execution_from_cache(self):
        with tempfile.TemporaryDirectory() as output_dir:
            with runners.InteractiveMode(
                task_launcher=LocalEnvironmentLauncher(),
                root_uri=local_storage.LocalStorageProvider().make_uri(path=output_dir),
            ):
                result_art_1 = generate_random_number().outputs["Output"]
                # Waiting for the execution completion.
                # The execution is only cached when it's successfully finished.
                result_art_1.materialize()
                result_art_2 = generate_random_number().outputs["Output"]
                self.assertEqual(result_art_1.materialize(), result_art_2.materialize())

    def test_reusing_execution_from_cache_with_max_cache_staleness(self):
        # Testing the cache reuse rules.
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

        with tempfile.TemporaryDirectory() as output_dir:
            runner = runners.Runner(
                task_launcher=LocalEnvironmentLauncher(),
                root_uri=local_storage.LocalStorageProvider().make_uri(path=output_dir),
            )
            time_0 = datetime.datetime.utcnow()
            time_10 = time_0 + datetime.timedelta(days=10)
            time_12 = time_0 + datetime.timedelta(days=12)
            datetime_mock = mock.MagicMock(wraps=datetime)
            # We need to patch both runners and local_environment_launcher (which calculates end_time)
            # with mock.patch.object(runners, "datetime", wraps=datetime) as datetime_mock:
            with mock.patch.object(
                runners, attribute="datetime", new=datetime_mock
            ), mock.patch.object(
                local_environment_launcher, attribute="datetime", new=datetime_mock
            ):
                # Running the task that should become the oldest execution
                datetime_mock.datetime.utcnow.return_value = time_0
                result_time_0 = (
                    runner.run_component(generate_random_number)
                    .outputs["Output"]
                    .materialize()
                )
                # State:
                # oldest: time_0
                # latest: time_0
                # Testing that the execution (oldest) is reused when no max_cache_staleness is specified
                datetime_mock.datetime.utcnow.return_value = time_10
                result_1 = (
                    runner.run_component(generate_random_number)
                    .outputs["Output"]
                    .materialize()
                )
                self.assertEqual(
                    result_1,
                    result_time_0,
                    "Execution should have been reused from the oldest execution due to no max_cache_staleness.",
                )
                # State:
                # oldest: time_0
                # latest: time_0
                # Testing that the execution (latest=oldest) is reused when it's inside the range of max_cache_staleness
                datetime_mock.datetime.utcnow.return_value = time_10
                task_2 = generate_random_number()
                task_2.execution_options = structures.ExecutionOptionsSpec(
                    caching_strategy=structures.CachingStrategySpec(
                        max_cache_staleness="P30D"
                    )
                )
                result_2 = runner.run_task(task_2).outputs["Output"].materialize()
                self.assertEqual(
                    result_2,
                    result_time_0,
                    "Execution should have been reused as it's inside the range of max_cache_staleness.",
                )
                # State:
                # oldest: time_0
                # latest: time_0
                # P30D: time_0
                # At this point there should only be one execution in the cache.
                # Testing that the executions are not reused when they are outside of the range of max_cache_staleness
                datetime_mock.datetime.utcnow.return_value = time_10
                task3 = generate_random_number()
                task3.execution_options = structures.ExecutionOptionsSpec(
                    caching_strategy=structures.CachingStrategySpec(
                        max_cache_staleness="P5D"
                    )
                )
                result_time_10 = runner.run_task(task3).outputs["Output"].materialize()
                self.assertNotEqual(
                    result_time_10,
                    result_time_0,
                    "Execution should not have been reused as it's outside of the range of max_cache_staleness.",
                )
                # At this point there should be two executions in the cache.
                # State:
                # oldest: time_0
                # latest: time_10
                # P30D: time_0
                # P5D: time_10
                # Testing that the first cached execution candidate is the execution that had the same with max_cache_staleness (if within the time range).
                # In this case P30D is set to the "time_0" execution, so this is what should be reused.
                datetime_mock.datetime.utcnow.return_value = time_12
                task4 = generate_random_number()
                task4.execution_options = structures.ExecutionOptionsSpec(
                    caching_strategy=structures.CachingStrategySpec(
                        max_cache_staleness="P30D"
                    )
                )
                result_4 = runner.run_task(task4).outputs["Output"].materialize()
                self.assertEqual(
                    result_4,
                    result_time_0,
                    "The 'P30D==time_0' execution should have been reused.",
                )
                # State:
                # oldest: time_0
                # latest: time_10
                # P30D: time_0
                # P5D: time_10
                # Testing that the first cached execution candidate is the execution that had the same with max_cache_staleness (if within the time range).
                # In this case P5D is set to the "time_10" execution, so this is what should be reused.
                datetime_mock.datetime.utcnow.return_value = time_12
                task5 = generate_random_number()
                task5.execution_options = structures.ExecutionOptionsSpec(
                    caching_strategy=structures.CachingStrategySpec(
                        max_cache_staleness="P5D"
                    )
                )
                result_5 = runner.run_task(task5).outputs["Output"].materialize()
                self.assertEqual(
                    result_5,
                    result_time_10,
                    "The 'P5D==time_10' execution should have been reused.",
                )
                # State:
                # oldest: time_0
                # latest: time_10
                # P30D: time_0
                # P5D: time_10
                # Testing that the when there is no viable (within the time range) execution with same with max_cache_staleness, the latest execution is used (if viable).
                # In this case, the latest execution is viable.
                datetime_mock.datetime.utcnow.return_value = time_12
                task6 = generate_random_number()
                task6.execution_options = structures.ExecutionOptionsSpec(
                    caching_strategy=structures.CachingStrategySpec(
                        max_cache_staleness="P20D"
                    )
                )
                result_6 = runner.run_task(task6).outputs["Output"].materialize()
                self.assertEqual(
                    result_6,
                    result_time_10,
                    "The 'latest==time_10' execution should have been reused.",
                )
                # State:
                # oldest: time_0
                # latest: time_10
                # P30D: time_0
                # P5D: time_10
                # P20D: time_10
                # Testing that the when there is no viable (within the time range) execution with same with max_cache_staleness, the latest execution is used (if viable).
                # In this case, the latest execution is not viable, so a new execution is created.
                datetime_mock.datetime.utcnow.return_value = time_12
                task7 = generate_random_number()
                task7.execution_options = structures.ExecutionOptionsSpec(
                    caching_strategy=structures.CachingStrategySpec(
                        max_cache_staleness="P1D"
                    )
                )
                result_time_12 = runner.run_task(task7).outputs["Output"].materialize()
                self.assertNotEqual(
                    result_time_12,
                    result_time_0,
                    "The 'oldest==time_0' execution should not have been reused since it's out of time range.",
                )
                self.assertNotEqual(
                    result_time_12,
                    result_time_10,
                    "The 'latest==time_10' execution should not have been reused since it's out of time range.",
                )
                # State:
                # oldest: time_0
                # latest: time_12
                # P30D: time_0
                # P5D: time_10
                # P20D: time_10
                # P1D: time_12
                # Again, Testing that the first cached execution candidate is the execution that had the same with max_cache_staleness (if within the time range).
                # In this case P20D is set to the "time_10" execution, so this is what should be reused (despite time_0 and time_12 being in range).
                datetime_mock.datetime.utcnow.return_value = time_12
                task8 = generate_random_number()
                task8.execution_options = structures.ExecutionOptionsSpec(
                    caching_strategy=structures.CachingStrategySpec(
                        max_cache_staleness="P20D"
                    )
                )
                result_8 = runner.run_task(task8).outputs["Output"].materialize()
                self.assertEqual(
                    result_8,
                    result_time_10,
                    "The 'P5D==time_10' execution should have been reused.",
                )

    def test_constant_artifacts_are_deduplicated(self):
        # This test is probabilistic and might sometimes have false successes
        # Running the whole test multiple times to increase the chance of detection
        for _ in range(10):
            with tempfile.TemporaryDirectory() as output_dir:
                with runners.InteractiveMode(
                    task_launcher=LocalEnvironmentLauncher(),
                    root_uri=local_storage.LocalStorageProvider().make_uri(
                        path=output_dir
                    ),
                ):
                    # No need to launch many executions.
                    # After first artifact reuse, all later artifacts are reused.
                    # So the test becomes: "Was the 2nd execution's constant input artifact reused?"
                    execution_1 = produce_string(string="str", dummy="1")
                    execution_2 = produce_string(string="str", dummy="2")
                    execution_1.wait_for_completion()
                    execution_2.wait_for_completion()
                    self.assertEqual(
                        execution_1.input_arguments["string"]._uri_reader.uri.to_dict(),
                        execution_2.input_arguments["string"]._uri_reader.uri.to_dict(),
                    )

    def test_output_artifacts_are_deduplicated_across_executions(self):
        # This test is probabilistic and might sometimes have false successes
        # Running the whole test multiple times to increase the chance of detection
        for _ in range(10):
            with tempfile.TemporaryDirectory() as output_dir:
                with runners.InteractiveMode(
                    task_launcher=LocalEnvironmentLauncher(),
                    root_uri=local_storage.LocalStorageProvider().make_uri(
                        path=output_dir
                    ),
                ):
                    # No need to launch many executions.
                    # After first artifact reuse, all later artifacts are reused.
                    # So the test becomes: "Was the 2nd execution's output artifact reused?"
                    execution_1 = produce_string(dummy="1")
                    execution_2 = produce_string(dummy="2")
                    execution_1.wait_for_completion()
                    execution_2.wait_for_completion()
                    self.assertEqual(
                        execution_1.outputs["Output"]._uri_reader.uri.to_dict(),
                        execution_2.outputs["Output"]._uri_reader.uri.to_dict(),
                    )

    def test_output_artifacts_are_deduplicated_in_same_execution(self):
        with tempfile.TemporaryDirectory() as output_dir:
            with runners.InteractiveMode(
                task_launcher=LocalEnvironmentLauncher(),
                root_uri=local_storage.LocalStorageProvider().make_uri(path=output_dir),
            ):
                execution: runners.ContainerExecution = produce_two_equal_strings()
                execution.wait_for_completion()
                artifact_1: runners._StorageArtifact = execution.outputs["string1"]
                artifact_2: runners._StorageArtifact = execution.outputs["string2"]
                self.assertEqual(
                    artifact_1._uri_reader.uri.to_dict(),
                    artifact_2._uri_reader.uri.to_dict(),
                )

    def test_content_based_cache_reuse(self):
        """Tests that downstream execution can be reused from cache if the input artifact content is the same even if the upstream execution is not reused from cache.

        For example, if we fix a typo in the component code comments, that component will be re-executed.
        But if the outputs are the same the downstream components should still be reused from cache.
        """

        @components.create_component_from_func
        def make_seed_1() -> int:
            # Version 1 of the component
            return 4

        @components.create_component_from_func
        def make_seed_2() -> int:
            # Version 2 of the component (outputs same data)
            return 2 * 2

        with tempfile.TemporaryDirectory() as output_dir:
            with runners.InteractiveMode(
                task_launcher=LocalEnvironmentLauncher(),
                root_uri=local_storage.LocalStorageProvider().make_uri(path=output_dir),
            ):
                seed1_art = make_seed_1().outputs["Output"]
                result_1 = (
                    generate_random_number(dummy=seed1_art)
                    .outputs["Output"]
                    .materialize()
                )

                seed2_art = make_seed_2().outputs["Output"]
                result_2 = (
                    generate_random_number(dummy=seed2_art)
                    .outputs["Output"]
                    .materialize()
                )
                self.assertEqual(result_1, result_2)

    def test_reusing_running_execution_from_cache(self):
        # How we test:
        # We start multiple chains of tasks at the same time.
        # To make things harder each chain starts with a different seed execution (which returns same outputs).
        # If the algorithm works correctly, then only one execution is actually launched at each level of the chain.
        # If at any level a running execution is not reused, then the chains will diverge and the final results will be different.
        NUMBER_OF_CHAINS = 10
        LENGTH_OF_CHAIN = 10
        with tempfile.TemporaryDirectory() as output_dir:
            with runners.InteractiveMode(
                task_launcher=LocalEnvironmentLauncher(),
                root_uri=local_storage.LocalStorageProvider().make_uri(path=output_dir),
            ):
                # ! Need to make sure that each inner list is different variable. Cannot use `[[]] * LENGTH_OF_CHAIN``
                artifacts_at_level = [[] for _ in range(LENGTH_OF_CHAIN)]
                for chain_idx in range(NUMBER_OF_CHAINS):
                    # Start all chains with a a different task execution (different input arguments)
                    result = produce_integer(dummy=chain_idx).outputs["Output"]
                    for idx in range(LENGTH_OF_CHAIN):
                        result = wait_then_return_random(dummy=result).outputs["Output"]
                        artifacts_at_level[idx].append(result)
                for idx in range(LENGTH_OF_CHAIN):
                    unique_results = set(
                        artifact.materialize() for artifact in artifacts_at_level[idx]
                    )
                    assert len(unique_results) == 1

    def test_reusing_running_execution_from_cache_with_max_cache_staleness(self):
        # We test the following two rules:
        # 1. Do not reuse a running execution if it's not valid due to `max_cache_staleness`.
        # 2. When reusing a running execution, take the oldest one (likely to finish first; stability).
        with tempfile.TemporaryDirectory() as output_dir:
            runner = runners.Runner(
                task_launcher=LocalEnvironmentLauncher(),
                root_uri=local_storage.LocalStorageProvider().make_uri(path=output_dir),
            )
            task1 = wait_then_return_random(wait_seconds=4)
            execution1 = runner.run_task(task1)
            result_art1 = execution1.outputs["Output"]

            time.sleep(2)
            # The execution1 is 2 seconds old at this point, but still running.
            # This code won't reuse the execution1 since it's too stale (need to be no more than 1 seconds old).
            task2 = wait_then_return_random(wait_seconds=4)
            task2.execution_options = structures.ExecutionOptionsSpec(
                caching_strategy=structures.CachingStrategySpec(
                    max_cache_staleness="PT1S"
                )
            )
            execution2 = runner.run_task(task2)
            result_art2 = execution2.outputs["Output"]

            time.sleep(1)
            # The execution1 is 3 seconds old at this point, but still running.
            # The execution2 is 1 seconds old at this point, but still running.
            # This code will reuse the execution1 since it's the oldest running execution.
            task3 = wait_then_return_random(wait_seconds=4)
            execution3 = runner.run_task(task3)
            result_art3 = execution3.outputs["Output"]

            self.assertNotEqual(result_art2.materialize(), result_art1.materialize())
            self.assertEqual(result_art3.materialize(), result_art1.materialize())

    def test_container_execution_logging_when_running(self):
        with tempfile.TemporaryDirectory() as output_dir:
            with runners.InteractiveMode(
                task_launcher=LocalEnvironmentLauncher(),
                root_uri=local_storage.LocalStorageProvider().make_uri(path=output_dir),
            ):
                with self.assertLogs(runners._default_log_printer_logger) as logs:
                    execution: runners.ContainerExecution = (
                        write_text_to_stdout_and_stderr()
                    )
                    execution.wait_for_completion()
                assert any("stdout:Hello world" in line for line in logs.output)
                assert any("stderr:Hello world" in line for line in logs.output)

    def test_container_execution_logging_when_reusing_from_cache(self):
        with tempfile.TemporaryDirectory() as output_dir:
            with runners.InteractiveMode(
                task_launcher=LocalEnvironmentLauncher(),
                root_uri=local_storage.LocalStorageProvider().make_uri(path=output_dir),
            ):
                # Populating the cache
                write_text_to_stdout_and_stderr().wait_for_completion()

                with self.assertLogs(runners._default_log_printer_logger) as logs:
                    execution: runners.ContainerExecution = (
                        write_text_to_stdout_and_stderr()
                    )
                    execution.wait_for_completion()
                assert any("Reused execution log:" in line for line in logs.output)
                assert any("stdout:Hello world" in line for line in logs.output)
                assert any("stderr:Hello world" in line for line in logs.output)

    def test_container_execution_log_artifact_when_running(self):
        with tempfile.TemporaryDirectory() as output_dir:
            with runners.InteractiveMode(
                task_launcher=LocalEnvironmentLauncher(),
                root_uri=local_storage.LocalStorageProvider().make_uri(path=output_dir),
            ):
                execution: runners.ContainerExecution = (
                    write_text_to_stdout_and_stderr()
                )
                execution.wait_for_completion()
                log_text = execution._log_reader.download_as_text()
                assert "stdout:Hello world" in log_text
                assert "stderr:Hello world" in log_text

    def test_container_execution_log_artifact_when_failing(self):
        with tempfile.TemporaryDirectory() as output_dir:
            with runners.InteractiveMode(
                task_launcher=LocalEnvironmentLauncher(),
                root_uri=local_storage.LocalStorageProvider().make_uri(path=output_dir),
                # Preventing the context from raising ExecutionFailedError
                # Adding an outer with `self.assertRaises(runners.ExecutionFailedError)`
                # does not work since for some reason it catches all errors.
                wait_for_completion_on_exit=False,
            ):
                execution: runners.ContainerExecution = fail()
                with self.assertRaises(runners.ExecutionFailedError):
                    execution.wait_for_completion()
                log_text = execution._log_reader.download_as_text()
                self.assertIn("Failing...", log_text)
                assert "Failing..." in log_text

    def test_container_execution_log_artifact_when_reusing_from_cache(self):
        with tempfile.TemporaryDirectory() as output_dir:
            with runners.InteractiveMode(
                task_launcher=LocalEnvironmentLauncher(),
                root_uri=local_storage.LocalStorageProvider().make_uri(path=output_dir),
            ):
                # Populating the cache
                write_text_to_stdout_and_stderr().wait_for_completion()

                execution: runners.ContainerExecution = (
                    write_text_to_stdout_and_stderr()
                )
                execution.wait_for_completion()
                log_text = execution._log_reader.download_as_text()
                assert "stdout:Hello world" in log_text
                assert "stderr:Hello world" in log_text


if __name__ == "__main__":
    unittest.main(verbosity=2)
