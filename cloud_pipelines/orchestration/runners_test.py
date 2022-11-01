import tempfile
import unittest

# from ..components import create_component_from_func, create_graph_component_from_pipeline_func
from cloud_pipelines.components import (
    create_component_from_func,
    create_graph_component_from_pipeline_func,
)


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


def build_pipeline_1():
    def add(a: int, b: int) -> int:
        print("add(a={};b={})".format(a, b))
        return a + b

    add_op = create_component_from_func(add, base_image="python:3.8")

    task = add_op(3, 5)

    def pipeline1_func():
        task1 = add_op(3, 5)
        task2 = add_op(3, task1.outputs["Output"])

    pipeline_op = create_graph_component_from_pipeline_func(pipeline1_func)
    return pipeline_op


class LaunchersTestCase(unittest.TestCase):
    def test_local_environment_launcher(self):
        pipeline_op = build_pipeline_1()
        pipeline_task = pipeline_op()

        with tempfile.TemporaryDirectory() as output_dir:
            runners.run_container_tasks(
                task_specs=pipeline_task.component_ref.spec.implementation.graph._toposorted_tasks,
                graph_input_arguments=pipeline_task.arguments,
                task_launcher=LocalEnvironmentLauncher(),
                output_uri_generator=runners.LocalPathGenerator(output_dir),
            )

    def test_local_docker_launcher(self):
        pipeline_op = build_pipeline_1()
        pipeline_task = pipeline_op()

        with tempfile.TemporaryDirectory() as output_dir:
            runners.run_container_tasks(
                task_specs=pipeline_task.component_ref.spec.implementation.graph._toposorted_tasks,
                graph_input_arguments=pipeline_task.arguments,
                task_launcher=DockerContainerLauncher(),
                output_uri_generator=runners.LocalPathGenerator(output_dir),
            )

    def test_local_kubernetes_launcher(self):
        pipeline_op = build_pipeline_1()
        pipeline_task = pipeline_op()

        with tempfile.TemporaryDirectory() as output_dir:
            runners.run_container_tasks(
                task_specs=pipeline_task.component_ref.spec.implementation.graph._toposorted_tasks,
                graph_input_arguments=pipeline_task.arguments,
                task_launcher=LocalKubernetesContainerLauncher(),
                output_uri_generator=runners.LocalPathGenerator(output_dir),
            )


if __name__ == "__main__":
    unittest.main()
