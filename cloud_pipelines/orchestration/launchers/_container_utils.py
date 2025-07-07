from typing import List, Mapping, NamedTuple, Sequence, OrderedDict, Union

from ...components import structures
from ..._components.components import _data_passing as _internal_data_passing
from . import naming_utils


_inputs_dir = "/tmp/inputs"
_outputs_dir = "/tmp/outputs"
_single_io_file_name = "data"


def _generate_input_file_name(port_name):
    return f"{_inputs_dir}/{naming_utils.sanitize_file_name(port_name)}/{_single_io_file_name}"


def _generate_output_file_name(port_name):
    return f"{_outputs_dir}/{naming_utils.sanitize_file_name(port_name)}/{_single_io_file_name}"


_ResolvedCommandLineAndPaths = NamedTuple(
    "_ResolvedCommandLineAndPaths",
    [
        ("command", Sequence[str]),
        ("args", Sequence[str]),
        ("input_paths", Mapping[str, str]),
        ("output_paths", Mapping[str, str]),
        ("inputs_consumed_by_value", Mapping[str, str]),
    ],
)


def _resolve_command_line_and_paths(
    component_spec: structures.ComponentSpec,
    arguments: Mapping[str, str],
    input_path_generator=_generate_input_file_name,
    output_path_generator=_generate_output_file_name,
    argument_serializer=_internal_data_passing.serialize_value,
) -> _ResolvedCommandLineAndPaths:
    """Resolves the command line argument placeholders. Also produces the maps of the generated input/output paths."""
    argument_values = arguments

    if not isinstance(
        component_spec.implementation, structures.ContainerImplementation
    ):
        raise TypeError("Only container components have command line to resolve")

    inputs_dict = {
        input_spec.name: input_spec for input_spec in component_spec.inputs or []
    }
    container_spec = component_spec.implementation.container

    output_paths = (
        OrderedDict()
    )  # Preserving the order to make the kubernetes output names deterministic
    unconfigurable_output_paths = container_spec.file_outputs or {}
    for output in component_spec.outputs or []:
        if output.name in unconfigurable_output_paths:
            output_paths[output.name] = unconfigurable_output_paths[output.name]

    input_paths = OrderedDict()
    inputs_consumed_by_value = {}

    def expand_command_part(arg) -> Union[str, List[str], None]:
        if arg is None:
            return None
        if isinstance(arg, (str, int, float, bool)):
            return str(arg)

        if isinstance(arg, structures.InputValuePlaceholder):
            input_name = arg.input_name
            input_spec = inputs_dict[input_name]
            input_value = argument_values.get(input_name, None)
            if input_value is not None:
                serialized_argument = argument_serializer(input_value, input_spec.type)
                inputs_consumed_by_value[input_name] = serialized_argument
                return serialized_argument
            else:
                if input_spec.optional:
                    return None
                else:
                    raise ValueError(
                        "No value provided for input {}".format(input_name)
                    )

        if isinstance(arg, structures.InputPathPlaceholder):
            input_name = arg.input_name
            input_value = argument_values.get(input_name, None)
            if input_value is not None:
                input_path = input_path_generator(input_name)
                input_paths[input_name] = input_path
                return input_path
            else:
                input_spec = inputs_dict[input_name]
                if input_spec.optional:
                    # Even when we support default values there is no need to check for a default here.
                    # In current execution flow (called by python task factory), the missing argument would be replaced with the default value by python itself.
                    return None
                else:
                    raise ValueError(
                        "No value provided for input {}".format(input_name)
                    )

        elif isinstance(arg, structures.OutputPathPlaceholder):
            output_name = arg.output_name
            output_filename = output_path_generator(output_name)
            if arg.output_name in output_paths:
                if output_paths[output_name] != output_filename:
                    raise ValueError(
                        "Conflicting output files specified for port {}: {} and {}".format(
                            output_name, output_paths[output_name], output_filename
                        )
                    )
            else:
                output_paths[output_name] = output_filename

            return output_filename

        elif isinstance(arg, structures.ConcatPlaceholder):
            expanded_argument_strings = expand_argument_list(arg.items)
            return "".join(expanded_argument_strings)

        elif isinstance(arg, structures.IfPlaceholder):
            arg = arg.if_structure
            condition_result = expand_command_part(arg.condition)
            # Python gotcha: `bool("False") == True`. So we need to use `_deserialize_boolean`.
            condition_result_bool = _deserialize_boolean(condition_result)
            result_node = arg.then_value if condition_result_bool else arg.else_value
            if result_node is None:
                return []
            if isinstance(result_node, list):
                expanded_result = expand_argument_list(result_node)
            else:
                expanded_result = expand_command_part(result_node)
            return expanded_result

        elif isinstance(arg, structures.IsPresentPlaceholder):
            argument_is_present = argument_values.get(arg.input_name, None) is not None
            return str(argument_is_present)
        else:
            raise TypeError("Unrecognized argument type: {}".format(arg))

    def expand_argument_list(argument_list):
        expanded_list = []
        if argument_list is not None:
            for part in argument_list:
                expanded_part = expand_command_part(part)
                if expanded_part is not None:
                    if isinstance(expanded_part, list):
                        expanded_list.extend(expanded_part)
                    else:
                        expanded_list.append(str(expanded_part))
        return expanded_list

    expanded_command = expand_argument_list(container_spec.command)
    expanded_args = expand_argument_list(container_spec.args)

    return _ResolvedCommandLineAndPaths(
        command=expanded_command,
        args=expanded_args,
        input_paths=input_paths,
        output_paths=output_paths,
        inputs_consumed_by_value=inputs_consumed_by_value,
    )


def _deserialize_boolean(string: str) -> bool:
    string = string.lower()
    if string == "true":
        return True
    elif string == "false":
        return False
    else:
        raise ValueError(f"Invalid serialized boolean value: {string}")
