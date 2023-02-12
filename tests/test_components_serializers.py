import shutil
import tempfile
import unittest

from cloud_pipelines.components import create_component_from_func, InputPath, OutputPath
from cloud_pipelines.orchestration import runners
from cloud_pipelines.orchestration.launchers.local_environment_launcher import (
    LocalEnvironmentLauncher,
)


def create_local_interactive_mode(root_dir: str):
    return runners.InteractiveMode(
        task_launcher=LocalEnvironmentLauncher(),
        root_uri=root_dir,
    )


class ComponentsSerializersTestCase(unittest.TestCase):
    def setUp(self):
        self._temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self._temp_dir)

    def test_PythonPickle(self):
        def transform_pickle(
            object_path: InputPath("PythonPickle"),
            output_object_path: OutputPath("PythonPickle"),
        ):
            import pickle

            with open(object_path, "rb") as input_file:
                obj = pickle.load(file=input_file)
            with open(output_object_path, "wb") as output_file:
                pickle.dump(obj=obj, file=output_file)

        transform_pickle_op = create_component_from_func(
            func=transform_pickle,
        )

        with create_local_interactive_mode(root_dir=self._temp_dir):
            input_obj = range(1, 10, 3)
            output_obj_art = transform_pickle_op(object=input_obj).outputs[
                "output_object"
            ]
            output_obj = output_obj_art.materialize()
            assert output_obj
            assert isinstance(output_obj, range)
            assert list(output_obj) == [1, 4, 7]


if __name__ == "__main__":
    unittest.main(verbosity=2)
