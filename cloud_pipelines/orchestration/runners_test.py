import os
import tempfile
import unittest

# from ..components import create_component_from_func, create_graph_component_from_pipeline_func
from cloud_pipelines import components


from cloud_pipelines.orchestration import runners

# from .launchers.local_environment_launcher import LocalEnvironmentContainerLauncher
# from .launchers.local_docker_launcher import DockerContainerLauncher
# from .launchers.local_kubernetes_launcher import LocalKubernetesContainerLauncher
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


def _build_data_passing_pipeline_task():
    data_passing_op = _build_data_passing_graph_component()
    return data_passing_op(graph_input_1="graph_input_1")


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


def _build_nested_graph_pipeline_task():
    nested_pipeline_op = _build_nested_graph_component()
    return nested_pipeline_op(outer_graph_input_1="outer_graph_input_1")


@components.create_component_from_func
def produce_value() -> str:
    print("Component: produce_value")
    return "produce_value"


@components.create_component_from_func
def consume_as_value(data):
    print("Component: consume_as_value: " + data)


class LaunchersTestCase(unittest.TestCase):
    def test_local_environment_launcher(self):
        pipeline_task = _build_nested_graph_pipeline_task()

        with tempfile.TemporaryDirectory() as output_dir:
            runner = runners.Runner(
                task_launcher=LocalEnvironmentLauncher(),
                root_uri=local_storage.LocalStorageProvider().make_uri(path=output_dir),
            )
            execution = runner.run_task(
                task_spec=pipeline_task,
            )
            execution.wait_for_completion()

    def test_local_docker_launcher(self):
        pipeline_task = _build_nested_graph_pipeline_task()

        with tempfile.TemporaryDirectory() as output_dir:
            runner = runners.Runner(
                task_launcher=DockerContainerLauncher(),
                root_uri=local_storage.LocalStorageProvider().make_uri(path=output_dir),
            )
            execution = runner.run_task(
                task_spec=pipeline_task,
            )
            execution.wait_for_completion()

    def test_local_kubernetes_launcher(self):
        pipeline_task = _build_nested_graph_pipeline_task()

        with tempfile.TemporaryDirectory() as output_dir:
            runner = runners.Runner(
                task_launcher=LocalKubernetesContainerLauncher(),
                root_uri=local_storage.LocalStorageProvider().make_uri(path=output_dir),
            )
            execution = runner.run_task(
                task_spec=pipeline_task,
            )
            execution.wait_for_completion()

    @unittest.skipUnless(
        condition=_GOOGLE_CLOUD_STORAGE_ROOT_URI is None,
        reason="Root GCS URI is not set",
    )
    def test_google_cloud_batch_launcher(self):
        from cloud_pipelines.orchestration.launchers.google_cloud_batch_launcher import (
            GoogleCloudBatchLauncher,
        )
        from cloud_pipelines.orchestration.storage_providers.google_cloud_storage import (
            GoogleCloudStorageProvider,
        )

        if not _GOOGLE_CLOUD_STORAGE_ROOT_URI:
            self.skipTest(reason="Root GCS URI is not set")
            return

        pipeline_task = _build_nested_graph_pipeline_task()

        runner = runners.Runner(
            task_launcher=GoogleCloudBatchLauncher(),
            root_uri=GoogleCloudStorageProvider().make_uri(
                uri=_GOOGLE_CLOUD_STORAGE_ROOT_URI
            ),
        )
        execution = runner.run_task(
            task_spec=pipeline_task,
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

if __name__ == "__main__":
    unittest.main(verbosity=2)
