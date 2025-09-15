import pathlib
import subprocess
import sys
import tempfile
import unittest
from contextlib import contextmanager
from pathlib import Path
from typing import Callable, NamedTuple, Optional, Sequence

from cloud_pipelines import components
from cloud_pipelines.components import InputPath, OutputPath
from cloud_pipelines.components import _lightweight
from cloud_pipelines.components._lightweight import (
    InputTextFile,
    InputBinaryFile,
    OutputTextFile,
    OutputBinaryFile,
)
from cloud_pipelines.components import structures
from cloud_pipelines.orchestration.launchers import _container_utils


@contextmanager
def components_override_input_output_dirs_context(
    inputs_dir: Optional[str] = None, outputs_dir: Optional[str] = None
):
    old_inputs_dir = _container_utils._inputs_dir
    old_outputs_dir = _container_utils._outputs_dir
    try:
        if inputs_dir:
            _container_utils._inputs_dir = inputs_dir
        if outputs_dir:
            _container_utils._outputs_dir = outputs_dir
        yield
    finally:
        _container_utils._inputs_dir = old_inputs_dir
        _container_utils._outputs_dir = old_outputs_dir


def _component_test_func_in_0_out_0():
    pass


def _component_test_func_add_two_numbers(a: float, b: float) -> float:
    """Returns sum of two arguments"""
    return a + b


class PythonOpTestCase(unittest.TestCase):
    def helper_test_2_in_1_out_component_using_local_call(
        self, func, op, arguments=[3.0, 5.0]
    ):
        expected = func(arguments[0], arguments[1])
        if isinstance(expected, tuple):
            expected = expected[0]
        expected_str = str(expected)

        with tempfile.TemporaryDirectory() as temp_dir_name:
            with components_override_input_output_dirs_context(
                outputs_dir=temp_dir_name
            ):
                task = op(arguments[0], arguments[1])
                resolved_cmd = _container_utils._resolve_command_line_and_paths(
                    task.component_ref.spec,
                    task.arguments,
                )

            full_command = resolved_cmd.command + resolved_cmd.args
            # TODO: Transform the command-line to be OS-agnostic

            # Setting the component python interpreter to the current one.
            # Otherwise the components are executed in different environment.
            # Some components (e.g. the ones that use code pickling) are sensitive to this.
            for i in range(5):
                if "python3" in full_command[i]:
                    # Lightweight components use Shell to call Python. So we convert the path to POSIX.
                    # The simple, but less robust alternative is: `python_path = "python"`
                    python_path = pathlib.Path(sys.executable).as_posix()
                    full_command[i] = full_command[i].replace("python3", python_path)
                    break
            subprocess.run(full_command, check=True)

            output_path = list(resolved_cmd.output_paths.values())[0]
            actual_str = Path(output_path).read_text()

        self.assertEqual(float(actual_str), float(expected_str))

    def helper_test_2_in_2_out_component_using_local_call(self, func, op, output_names):
        arg1 = float(3)
        arg2 = float(5)

        expected_tuple = func(arg1, arg2)
        expected1_str = str(expected_tuple[0])
        expected2_str = str(expected_tuple[1])

        with tempfile.TemporaryDirectory() as temp_dir_name:
            with components_override_input_output_dirs_context(
                outputs_dir=temp_dir_name
            ):
                task = op(arg1, arg2)
                resolved_cmd = _container_utils._resolve_command_line_and_paths(
                    task.component_ref.spec,
                    task.arguments,
                )

            full_command = resolved_cmd.command + resolved_cmd.args
            # TODO: Transform the command-line to be OS-agnostic

            # Setting the component python interpreter to the current one.
            # Otherwise the components are executed in different environment.
            # Some components (e.g. the ones that use code pickling) are sensitive to this.
            for i in range(5):
                if "python3" in full_command[i]:
                    # Lightweight components use Shell to call Python. So we convert the path to POSIX.
                    # The simple, but less robust alternative is: `python_path = "python"`
                    python_path = pathlib.Path(sys.executable).as_posix()
                    full_command[i] = full_command[i].replace("python3", python_path)
                    break

            subprocess.run(full_command, check=True)

            (output_path1, output_path2) = (
                resolved_cmd.output_paths[output_names[0]],
                resolved_cmd.output_paths[output_names[1]],
            )
            actual1_str = Path(output_path1).read_text()
            actual2_str = Path(output_path2).read_text()

        self.assertEqual(float(actual1_str), float(expected1_str))
        self.assertEqual(float(actual2_str), float(expected2_str))

    def helper_test_component_created_from_func_using_local_call(
        self, func: Callable, arguments: dict
    ):
        task_factory = components.create_component_from_func(func)
        self.helper_test_component_against_func_using_local_call(
            func, task_factory, arguments
        )

    def helper_test_component_against_func_using_local_call(
        self, func: Callable, op: Callable, arguments: dict
    ):
        # ! This function cannot be used when component has output types that use custom serialization since it will compare non-serialized function outputs with serialized component outputs.
        # Evaluating the function to get the expected output values
        expected_output_values_list = func(**arguments)
        if not isinstance(expected_output_values_list, Sequence) or isinstance(
            expected_output_values_list, str
        ):
            expected_output_values_list = [str(expected_output_values_list)]
        expected_output_values_list = [
            str(value) for value in expected_output_values_list
        ]

        output_names = [output.name for output in op.component_spec.outputs]
        expected_output_values_dict = dict(
            zip(output_names, expected_output_values_list)
        )

        self.helper_test_component_using_local_call(
            op, arguments, expected_output_values_dict
        )

    def helper_test_component_using_local_call(
        self,
        component_task_factory: Callable,
        arguments: Optional[dict] = None,
        expected_output_values: Optional[dict] = None,
    ):
        arguments = arguments or {}
        expected_output_values = expected_output_values or {}
        with tempfile.TemporaryDirectory() as temp_dir_name:
            # Creating task from the component.
            # We do it in a special context that allows us to control the output file locations.
            inputs_path = Path(temp_dir_name) / "inputs"
            outputs_path = Path(temp_dir_name) / "outputs"
            with components_override_input_output_dirs_context(
                str(inputs_path), str(outputs_path)
            ):
                task = component_task_factory(**arguments)
                resolved_cmd = _container_utils._resolve_command_line_and_paths(
                    task.component_ref.spec,
                    task.arguments,
                )

            # Preparing input files
            for input_name, input_file_path in (resolved_cmd.input_paths or {}).items():
                Path(input_file_path).parent.mkdir(parents=True, exist_ok=True)
                Path(input_file_path).write_text(str(arguments[input_name]))

            # Constructing the full command-line from resolved command+args
            full_command = resolved_cmd.command + resolved_cmd.args
            # Setting the component python interpreter to the current one.
            # Otherwise the components are executed in different environment.
            # Some components (e.g. the ones that use code pickling) are sensitive to this.
            for i in range(2):
                if full_command[i] == "python3":
                    full_command[i] = sys.executable

            # Executing the command-line locally
            subprocess.run(full_command, check=True)

            actual_output_values_dict = {
                output_name: Path(output_path).read_text()
                for output_name, output_path in resolved_cmd.output_paths.items()
            }

        self.assertDictEqual(actual_output_values_dict, expected_output_values)

    def test_create_component_from_func_local_call(self):
        func = _component_test_func_add_two_numbers
        op = components.create_component_from_func(func)

        self.helper_test_2_in_1_out_component_using_local_call(func, op)

    def test_create_component_from_func_output_component_file(self):
        func = _component_test_func_add_two_numbers
        with tempfile.TemporaryDirectory() as temp_dir_name:
            component_path = str(Path(temp_dir_name) / "component.yaml")
            components.create_component_from_func(
                func, output_component_file=component_path
            )
            op = components.load_component_from_file(component_path)

        self.helper_test_2_in_1_out_component_using_local_call(func, op)

    def test_indented_create_component_from_func_local_call(self):
        def add_two_numbers_indented(a: float, b: float) -> float:
            """Returns sum of two arguments"""
            return a + b

        func = add_two_numbers_indented
        op = components.create_component_from_func(func)

        self.helper_test_2_in_1_out_component_using_local_call(func, op)

    def test_create_component_from_func_multiple_named_typed_outputs(self):
        from typing import NamedTuple

        def add_multiply_two_numbers(
            a: float, b: float
        ) -> NamedTuple("DummyName", [("sum", float), ("product", float)]):
            """Returns sum and product of two arguments"""
            return (a + b, a * b)

        func = add_multiply_two_numbers
        op = components.create_component_from_func(func)

        self.helper_test_2_in_2_out_component_using_local_call(
            func, op, output_names=["sum", "product"]
        )

    def test_extract_component_interface(self):
        from typing import NamedTuple

        def my_func(
            required_param,
            int_param: int = 42,
            float_param: float = 3.14,
            str_param: str = "string",
            bool_param: bool = True,
            none_param=None,
            optional_str_param: Optional[str] = None,  # typing.Union[str, None] = same
            custom_type_param: "Custom type" = None,
            custom_struct_type_param: {
                "CustomType": {"param1": "value1", "param2": "value2"}
            } = None,
        ) -> NamedTuple(
            "DummyName",
            [
                # ("required_param",),  # All typing.NamedTuple fields must have types
                ("int_param", int),
                ("float_param", float),
                ("str_param", str),
                ("bool_param", bool),
                # (
                #     "custom_type_param",
                #     "Custom type",
                # ),  # SyntaxError: Forward reference must be an expression -- got 'Custom type'
                ("custom_type_param", "CustomType"),
                # (
                #     "custom_struct_type_param",
                #     {"CustomType": {"param1": "value1", "param2": "value2"}},
                # ),  # TypeError: NamedTuple('Name', [(f0, t0), (f1, t1), ...]); each t must be a type Got {'CustomType': {'param1': 'value1', 'param2': 'value2'}}
            ],
        ):
            """Function docstring"""
            pass

        component_spec = _lightweight._extract_component_interface(my_func)

        self.assertEqual(
            component_spec.inputs,
            [
                structures.InputSpec(name="required_param"),
                structures.InputSpec(
                    name="int_param", type="Integer", default="42", optional=True
                ),
                structures.InputSpec(
                    name="float_param", type="Float", default="3.14", optional=True
                ),
                structures.InputSpec(
                    name="str_param", type="String", default="string", optional=True
                ),
                structures.InputSpec(
                    name="bool_param", type="Boolean", default="True", optional=True
                ),
                structures.InputSpec(
                    name="none_param", optional=True
                ),  # No default='None'
                structures.InputSpec(
                    name="optional_str_param", type="String", optional=True
                ),
                structures.InputSpec(
                    name="custom_type_param", type="Custom type", optional=True
                ),
                structures.InputSpec(
                    name="custom_struct_type_param",
                    type={"CustomType": {"param1": "value1", "param2": "value2"}},
                    optional=True,
                ),
            ],
        )
        self.assertEqual(
            component_spec.outputs,
            [
                structures.OutputSpec(name="int_param", type="Integer"),
                structures.OutputSpec(name="float_param", type="Float"),
                structures.OutputSpec(name="str_param", type="String"),
                structures.OutputSpec(name="bool_param", type="Boolean"),
                # structures.OutputSpec(name='custom_type_param', type='Custom type', default='None'),
                structures.OutputSpec(name="custom_type_param", type="CustomType"),
                # TODO: Implement this behavior
                # structures.OutputSpec(
                #     name="custom_struct_type_param",
                #     type={"CustomType": {"param1": "value1", "param2": "value2"}},
                # ),
            ],
        )

        self.maxDiff = None
        self.assertDictEqual(
            component_spec.to_dict(),
            {
                "name": "My func",
                "description": "Function docstring",
                "inputs": [
                    {"name": "required_param"},
                    {
                        "name": "int_param",
                        "type": "Integer",
                        "default": "42",
                        "optional": True,
                    },
                    {
                        "name": "float_param",
                        "type": "Float",
                        "default": "3.14",
                        "optional": True,
                    },
                    {
                        "name": "str_param",
                        "type": "String",
                        "default": "string",
                        "optional": True,
                    },
                    {
                        "name": "bool_param",
                        "type": "Boolean",
                        "default": "True",
                        "optional": True,
                    },
                    {"name": "none_param", "optional": True},  # No default='None'
                    {
                        "name": "optional_str_param",
                        "type": "String",
                        "optional": True,
                    },
                    {
                        "name": "custom_type_param",
                        "type": "Custom type",
                        "optional": True,
                    },
                    {
                        "name": "custom_struct_type_param",
                        "type": {
                            "CustomType": {"param1": "value1", "param2": "value2"}
                        },
                        "optional": True,
                    },
                ],
                "outputs": [
                    {"name": "int_param", "type": "Integer"},
                    {"name": "float_param", "type": "Float"},
                    {"name": "str_param", "type": "String"},
                    {"name": "bool_param", "type": "Boolean"},
                    {"name": "custom_type_param", "type": "CustomType"},
                    # {'name': 'custom_struct_type_param', 'type': {'CustomType': {'param1': 'value1', 'param2': 'value2'}}, 'optional': True},
                ],
            },
        )

    def test_create_component_from_func_named_typed_outputs_with_underscores(self):
        from typing import NamedTuple

        def add_two_numbers_name2(
            a: float, b: float
        ) -> NamedTuple("DummyName", [("output_data", float)]):
            """Returns sum of two arguments"""
            return (a + b,)

        func = add_two_numbers_name2
        op = components.create_component_from_func(func)

        self.helper_test_2_in_1_out_component_using_local_call(func, op)

    def test_handling_same_input_output_names(self):
        from typing import NamedTuple

        def add_multiply_two_numbers(
            a: float, b: float
        ) -> NamedTuple("DummyName", [("a", float), ("b", float)]):
            """Returns sum and product of two arguments"""
            return (a + b, a * b)

        func = add_multiply_two_numbers
        op = components.create_component_from_func(func)

        self.helper_test_2_in_2_out_component_using_local_call(
            func, op, output_names=["a", "b"]
        )

    def test_handling_same_input_default_output_names(self):
        def add_two_numbers_indented(a: float, Output: float) -> float:
            """Returns sum of two arguments"""
            return a + Output

        func = add_two_numbers_indented
        op = components.create_component_from_func(func)

        self.helper_test_2_in_1_out_component_using_local_call(func, op)

    def test_saving_default_values(self):
        from typing import NamedTuple

        def add_multiply_two_numbers(
            a: float = 3, b: float = 5
        ) -> NamedTuple("DummyName", [("sum", float), ("product", float)]):
            """Returns sum and product of two arguments"""
            return (a + b, a * b)

        func = add_multiply_two_numbers
        component_spec = _lightweight._create_component_spec_from_func(func)

        self.assertEqual(component_spec.inputs[0].default, "3")
        self.assertEqual(component_spec.inputs[1].default, "5")

    def test_handling_of_descriptions(self):

        def some_function(
            param1: str,
            param2: str,
            param3: str,
        ) -> None:
            """
            Component description

            Args:
                param1: param1 description
                param2: param2 description
            """

        component_spec = _lightweight._create_component_spec_from_func(some_function)
        self.assertEqual(component_spec.description, "Component description")
        self.assertEqual(component_spec.inputs[0].description, "param1 description")
        self.assertEqual(component_spec.inputs[1].description, "param2 description")
        self.assertIsNone(component_spec.inputs[2].description)

    def test_handling_default_value_of_none(self):
        def assert_is_none(arg=None):
            assert arg is None

        func = assert_is_none
        op = components.create_component_from_func(func)
        self.helper_test_component_using_local_call(op)

    def test_handling_complex_default_values(self):
        def assert_values_are_default(
            singleton_param=None,
            function_param=ascii,
            dict_param: dict = {"b": [2, 3, 4]},
            func_call_param="_".join(["a", "b", "c"]),
        ):
            assert singleton_param is None
            assert function_param is ascii
            assert dict_param == {"b": [2, 3, 4]}
            assert func_call_param == "_".join(["a", "b", "c"])

        func = assert_values_are_default
        op = components.create_component_from_func(func)
        self.helper_test_component_using_local_call(op)

    def test_handling_boolean_arguments(self):
        def assert_values_are_true_false(
            bool1: bool,
            bool2: bool,
        ) -> int:
            assert bool1 is True
            assert bool2 is False
            return 1

        func = assert_values_are_true_false
        op = components.create_component_from_func(func)
        self.helper_test_2_in_1_out_component_using_local_call(
            func, op, arguments=[True, False]
        )

    def test_handling_list_dict_arguments(self):
        def assert_values_are_same(
            list_param: list,
            dict_param: dict,
        ) -> int:
            import unittest

            unittest.TestCase().assertEqual(
                list_param, ["string", 1, 2.2, True, False, None, [3, 4], {"s": 5}]
            )
            unittest.TestCase().assertEqual(
                dict_param,
                {
                    "str": "string",
                    "int": 1,
                    "float": 2.2,
                    "false": False,
                    "true": True,
                    "none": None,
                    "list": [3, 4],
                    "dict": {"s": 4},
                },
            )
            return 1

        # ! JSON map keys are always strings. Python converts all keys to strings without warnings
        func = assert_values_are_same
        op = components.create_component_from_func(func)
        self.helper_test_2_in_1_out_component_using_local_call(
            func,
            op,
            arguments=[
                ["string", 1, 2.2, True, False, None, [3, 4], {"s": 5}],
                {
                    "str": "string",
                    "int": 1,
                    "float": 2.2,
                    "false": False,
                    "true": True,
                    "none": None,
                    "list": [3, 4],
                    "dict": {"s": 4},
                },
            ],
        )

    def test_fail_on_handling_list_arguments_containing_python_objects(self):
        """Checks that lists containing python objects not having .to_struct() raise error during serialization."""

        class MyClass:
            pass

        def consume_list(
            list_param: list,
        ) -> int:
            return 1

        def consume_dict(
            dict_param: dict,
        ) -> int:
            return 1

        list_op = components.create_component_from_func(consume_list)
        dict_op = components.create_component_from_func(consume_dict)

        with self.assertRaises(Exception):
            list_op([1, MyClass(), 3])

        with self.assertRaises(Exception):
            dict_op({"k1": MyClass()})

    def test_handling_list_arguments_containing_serializable_python_objects(self):
        """Checks that lists containing python objects with .to_struct() can be properly serialized."""

        class MyClass:
            def to_struct(self):
                return {"foo": [7, 42]}

        def assert_values_are_correct(
            list_param: list,
            dict_param: dict,
        ) -> int:
            import unittest

            unittest.TestCase().assertEqual(list_param, [1, {"foo": [7, 42]}, 3])
            unittest.TestCase().assertEqual(dict_param, {"k1": {"foo": [7, 42]}})
            return 1

        task_factory = components.create_component_from_func(assert_values_are_correct)

        self.helper_test_component_using_local_call(
            task_factory,
            arguments=dict(
                list_param=[1, MyClass(), 3],
                dict_param={"k1": MyClass()},
            ),
            expected_output_values={"Output": "1"},
        )

    def test_handling_base64_pickle_arguments(self):
        def assert_values_are_same(
            obj1: "Base64Pickle",  # noqa: F821
            obj2: "Base64Pickle",  # noqa: F821
        ) -> int:
            import unittest

            unittest.TestCase().assertEqual(obj1["self"], obj1)
            unittest.TestCase().assertEqual(obj2, open)
            return 1

        func = assert_values_are_same
        op = components.create_component_from_func(func)

        recursive_obj = {}
        recursive_obj["self"] = recursive_obj
        self.helper_test_2_in_1_out_component_using_local_call(
            func,
            op,
            arguments=[
                recursive_obj,
                open,
            ],
        )

    def test_handling_list_dict_output_values(self):
        def produce_list() -> list:
            return ["string", 1, 2.2, True, False, None, [3, 4], {"s": 5}]

        # ! JSON map keys are always strings. Python converts all keys to strings without warnings
        task_factory = components.create_component_from_func(produce_list)

        import json

        expected_output = json.dumps(
            ["string", 1, 2.2, True, False, None, [3, 4], {"s": 5}]
        )

        self.helper_test_component_using_local_call(
            task_factory,
            arguments={},
            expected_output_values={"Output": expected_output},
        )

    def test_input_path(self):
        def consume_file_path(number_file_path: InputPath(int)) -> int:
            with open(number_file_path) as f:
                string_data = f.read()
            return int(string_data)

        task_factory = components.create_component_from_func(consume_file_path)

        self.assertEqual(task_factory.component_spec.inputs[0].type, "Integer")

        self.helper_test_component_using_local_call(
            task_factory,
            arguments={"number": "42"},
            expected_output_values={"Output": "42"},
        )

    # InputTextFile is deprecated
    def test_input_text_file(self):
        def consume_file_path(number_file: InputTextFile(int)) -> int:
            string_data = number_file.read()
            assert isinstance(string_data, str)
            return int(string_data)

        task_factory = components.create_component_from_func(consume_file_path)

        self.assertEqual(task_factory.component_spec.inputs[0].type, "Integer")

        self.helper_test_component_using_local_call(
            task_factory,
            arguments={"number": "42"},
            expected_output_values={"Output": "42"},
        )

    # InputBinaryFile is deprecated
    def test_input_binary_file(self):
        def consume_file_path(number_file: InputBinaryFile(int)) -> int:
            bytes_data = number_file.read()
            assert isinstance(bytes_data, bytes)
            return int(bytes_data)

        task_factory = components.create_component_from_func(consume_file_path)

        self.assertEqual(task_factory.component_spec.inputs[0].type, "Integer")

        self.helper_test_component_using_local_call(
            task_factory,
            arguments={"number": "42"},
            expected_output_values={"Output": "42"},
        )

    def test_output_path(self):
        def write_to_file_path(number_file_path: OutputPath(int)):
            with open(number_file_path, "w") as f:
                f.write(str(42))

        task_factory = components.create_component_from_func(write_to_file_path)

        self.assertFalse(task_factory.component_spec.inputs)
        self.assertEqual(len(task_factory.component_spec.outputs), 1)
        self.assertEqual(task_factory.component_spec.outputs[0].type, "Integer")

        self.helper_test_component_using_local_call(
            task_factory, arguments={}, expected_output_values={"number": "42"}
        )

    def test_output_text_file(self):
        def write_to_file_path(number_file: OutputTextFile(int)):
            number_file.write(str(42))

        task_factory = components.create_component_from_func(write_to_file_path)

        self.assertFalse(task_factory.component_spec.inputs)
        self.assertEqual(len(task_factory.component_spec.outputs), 1)
        self.assertEqual(task_factory.component_spec.outputs[0].type, "Integer")

        self.helper_test_component_using_local_call(
            task_factory, arguments={}, expected_output_values={"number": "42"}
        )

    # OutputBinaryFile is deprecated
    def test_output_binary_file(self):
        def write_to_file_path(number_file: OutputBinaryFile(int)):
            number_file.write(b"42")

        task_factory = components.create_component_from_func(write_to_file_path)

        self.assertFalse(task_factory.component_spec.inputs)
        self.assertEqual(len(task_factory.component_spec.outputs), 1)
        self.assertEqual(task_factory.component_spec.outputs[0].type, "Integer")

        self.helper_test_component_using_local_call(
            task_factory, arguments={}, expected_output_values={"number": "42"}
        )

    def test_output_path_plus_return_value(self):
        def write_to_file_path(number_file_path: OutputPath(int)) -> str:
            with open(number_file_path, "w") as f:
                f.write(str(42))
            return "Hello"

        task_factory = components.create_component_from_func(write_to_file_path)

        self.assertFalse(task_factory.component_spec.inputs)
        self.assertEqual(len(task_factory.component_spec.outputs), 2)
        self.assertEqual(task_factory.component_spec.outputs[0].type, "Integer")
        self.assertEqual(task_factory.component_spec.outputs[1].type, "String")

        self.helper_test_component_using_local_call(
            task_factory,
            arguments={},
            expected_output_values={"number": "42", "Output": "Hello"},
        )

    # InputTextFile and OutputTextFile are deprecated
    def test_all_data_passing_ways(self):
        def write_to_file_path(
            file_input1_path: InputPath(str),
            file_input2_file: InputTextFile(str),
            file_output1_path: OutputPath(str),
            file_output2_file: OutputTextFile(str),
            value_input1: str = "foo",
            value_input2: str = "foo",
        ) -> NamedTuple(
            "Outputs",
            [
                ("return_output1", str),
                ("return_output2", str),
            ],
        ):
            with open(file_input1_path, "r") as file_input1_file:
                with open(file_output1_path, "w") as file_output1_file:
                    file_output1_file.write(file_input1_file.read())

            file_output2_file.write(file_input2_file.read())

            return (value_input1, value_input2)

        task_factory = components.create_component_from_func(write_to_file_path)

        self.assertEqual(
            set(input.name for input in task_factory.component_spec.inputs),
            {"file_input1", "file_input2", "value_input1", "value_input2"},
        )
        self.assertEqual(
            set(output.name for output in task_factory.component_spec.outputs),
            {"file_output1", "file_output2", "return_output1", "return_output2"},
        )

        self.helper_test_component_using_local_call(
            task_factory,
            arguments={
                "file_input1": "file_input1_value",
                "file_input2": "file_input2_value",
                "value_input1": "value_input1_value",
                "value_input2": "value_input2_value",
            },
            expected_output_values={
                "file_output1": "file_input1_value",
                "file_output2": "file_input2_value",
                "return_output1": "value_input1_value",
                "return_output2": "value_input2_value",
            },
        )

    def test_optional_input_path(self):
        def consume_file_path(number_file_path: InputPath(int) = None) -> int:
            result = -1
            if number_file_path:
                with open(number_file_path) as f:
                    string_data = f.read()
                    result = int(string_data)
            return result

        task_factory = components.create_component_from_func(consume_file_path)

        self.helper_test_component_using_local_call(
            task_factory, arguments={}, expected_output_values={"Output": "-1"}
        )

        self.helper_test_component_using_local_call(
            task_factory,
            arguments={"number": "42"},
            expected_output_values={"Output": "42"},
        )

    def test_fail_on_input_path_non_none_default(self):
        def read_from_file_path(file_path: InputPath(int) = "/tmp/something") -> str:
            return file_path

        with self.assertRaises(ValueError):
            task_factory = components.create_component_from_func(read_from_file_path)

    def test_fail_on_output_path_default(self):
        def write_to_file_path(file_path: OutputPath(int) = None) -> str:
            return file_path

        with self.assertRaises(ValueError):
            task_factory = components.create_component_from_func(write_to_file_path)

    def test_annotations_stripping(self):
        import typing
        import collections

        MyFuncOutputs = typing.NamedTuple("Outputs", [("sum", int), ("product", int)])

        class CustomType1:
            pass

        def my_func(
            param1: CustomType1 = None,  # This caused failure previously
            param2: collections.OrderedDict = None,  # This caused failure previously
        ) -> MyFuncOutputs:  # This caused failure previously
            assert param1 == None
            assert param2 == None
            return (8, 15)

        task_factory = components.create_component_from_func(my_func)

        self.helper_test_component_using_local_call(
            task_factory,
            arguments={},
            expected_output_values={"sum": "8", "product": "15"},
        )

    def test_file_input_name_conversion(self):
        # Checking the input name conversion rules for file inputs:
        # For InputPath, the "_path" suffix is removed
        # For Input*, the "_file" suffix is removed

        def consume_file_path(
            number: int,
            number_1a_path: str,
            number_1b_file: str,
            number_1c_file_path: str,
            number_1d_path_file: str,
            number_2a_path: InputPath(str),
            number_2b_file: InputPath(str),
            number_2c_file_path: InputPath(str),
            number_2d_path_file: InputPath(str),
            number_3a_path: InputTextFile(str),
            number_3b_file: InputTextFile(str),
            number_3c_file_path: InputTextFile(str),
            number_3d_path_file: InputTextFile(str),
            number_4a_path: InputBinaryFile(str),
            number_4b_file: InputBinaryFile(str),
            number_4c_file_path: InputBinaryFile(str),
            number_4d_path_file: InputBinaryFile(str),
            output_number_2a_path: OutputPath(str),
            output_number_2b_file: OutputPath(str),
            output_number_2c_file_path: OutputPath(str),
            output_number_2d_path_file: OutputPath(str),
            output_number_3a_path: OutputTextFile(str),
            output_number_3b_file: OutputTextFile(str),
            output_number_3c_file_path: OutputTextFile(str),
            output_number_3d_path_file: OutputTextFile(str),
            output_number_4a_path: OutputBinaryFile(str),
            output_number_4b_file: OutputBinaryFile(str),
            output_number_4c_file_path: OutputBinaryFile(str),
            output_number_4d_path_file: OutputBinaryFile(str),
        ):
            pass

        task_factory = components.create_component_from_func(consume_file_path)
        actual_input_names = [
            input.name for input in task_factory.component_spec.inputs
        ]
        actual_output_names = [
            output.name for output in task_factory.component_spec.outputs
        ]

        self.assertEqual(
            [
                "number",
                "number_1a_path",
                "number_1b_file",
                "number_1c_file_path",
                "number_1d_path_file",
                "number_2a",
                "number_2b",
                "number_2c",
                "number_2d_path",
                "number_3a_path",
                "number_3b",
                "number_3c_file_path",
                "number_3d_path",
                "number_4a_path",
                "number_4b",
                "number_4c_file_path",
                "number_4d_path",
            ],
            actual_input_names,
        )

        self.assertEqual(
            [
                "output_number_2a",
                "output_number_2b",
                "output_number_2c",
                "output_number_2d_path",
                "output_number_3a_path",
                "output_number_3b",
                "output_number_3c_file_path",
                "output_number_3d_path",
                "output_number_4a_path",
                "output_number_4b",
                "output_number_4c_file_path",
                "output_number_4d_path",
            ],
            actual_output_names,
        )

    def test_packages_to_install_feature(self):
        task_factory = components.create_component_from_func(
            _component_test_func_in_0_out_0, packages_to_install=["six", "pip"]
        )

        self.helper_test_component_using_local_call(
            task_factory, arguments={}, expected_output_values={}
        )

        task_factory2 = components.create_component_from_func(
            _component_test_func_in_0_out_0,
            packages_to_install=[
                "bad-package-0ee7cf93f396cd5072603dec154425cd53bf1c681c7c7605c60f8faf7799b901"
            ],
        )
        with self.assertRaises(Exception):
            self.helper_test_component_using_local_call(
                task_factory2, arguments={}, expected_output_values={}
            )

    def test_component_annotations(self):
        def some_func():
            pass

        annotations = {
            "key1": "value1",
            "key2": "value2",
        }
        task_factory = components.create_component_from_func(
            some_func, annotations=annotations
        )
        component_spec = task_factory.component_spec
        self.assertEqual(component_spec.metadata.annotations, annotations)

    def test_code_with_escapes(self):
        def my_func():
            "Hello \n world"

        task_factory = components.create_component_from_func(my_func)
        self.helper_test_component_using_local_call(
            task_factory, arguments={}, expected_output_values={}
        )

    def test_end_to_end_python_component_pipeline(self):
        # Defining the Python function
        def add(a: float, b: float) -> float:
            """Returns sum of two arguments"""
            return a + b

        with tempfile.TemporaryDirectory() as temp_dir_name:
            add_component_file = str(Path(temp_dir_name).joinpath("add.component.yaml"))

            # Converting the function to a component. Instantiate it to create a pipeline task
            add_op = components.create_component_from_func(
                add, base_image="python:3.5", output_component_file=add_component_file
            )

            # Checking that the component artifact is usable:
            add_op2 = components.load_component_from_file(add_component_file)

            # Building the pipeline
            def calc_pipeline(
                a1,
                a2="7",
                a3="17",
            ):
                task_1 = add_op(a1, a2)
                task_2 = add_op2(a1, a2)
                task_3 = add_op(task_1.outputs["Output"], task_2.outputs["Output"])
                task_4 = add_op2(task_3.outputs["Output"], a3)

            # Instantiating the pipeline:
            calc_pipeline(42)


if __name__ == "__main__":
    unittest.main()
