import pathlib
from typing import Any, Optional
import warnings

from .._components.components import _data_passing as _internal_data_passing
from .._components.components import _structures

from . import _serializers


def save(
    obj: Any,
    path: str,
    type_spec: Optional[_structures.TypeSpecType] = None,
) -> _structures.TypeSpecType:
    if not type_spec:
        type_spec = _internal_data_passing.get_canonical_type_struct_for_type(
            typ=type(obj)
        )
        if not type_spec:
            full_class_name = _get_full_class_name(obj)
            # TODO: Streamline the type -> type_spec -> type_name conversions
            type_spec = _serializers.python_type_name_to_type_spec.get(
                full_class_name, full_class_name
            )

        warnings.warn(
            f'Missing type spec was inferred as "{type_spec}" based on the object "{obj}".'
        )
    type_name = _type_spec_to_type_name(type_spec)

    saver = _serializers.savers.get(type_name)
    if saver:
        saver(obj, path)
        return type_spec

    serialized_value = _internal_data_passing.serialize_value(
        value=obj, type_name=type_name
    )
    pathlib.Path(path).write_text(serialized_value)
    return type_spec


def _type_spec_to_type_name(type_spec: _structures.TypeSpecType) -> str:
    if isinstance(type_spec, str):
        return type_spec
    elif isinstance(type_spec, dict) and len(type_spec) == 1:
        return next(iter(type_spec.keys()))
    else:
        raise TypeError(
            f'Unsupported type_spec "{type_spec}" of type "{type(type_spec)}"'
        )


def _get_full_class_name(obj) -> str:
    cls = type(obj)
    module = cls.__module__
    name = cls.__qualname__
    if module is not None and module != "__builtin__":
        name = module + "." + name
    return name


def _get_deserializer_for_type_spec(type_spec: _structures.TypeSpecType):
    type_name = _type_spec_to_type_name(type_spec=type_spec)
    type_name = _serializers._alias_type_name_to_canonical.get(type_name, type_name)
    deserializer = _serializers._deserializers.get(type_name)
    return deserializer


def load(
    path: str,
    type_spec: _structures.TypeSpecType,
) -> Any:
    type_name = _type_spec_to_type_name(type_spec)
    loader = _serializers.loaders.get(type_name)
    if loader:
        return loader(path)
    deserializer = _get_deserializer_for_type_spec(type_spec=type_spec)
    if not deserializer:
        raise ValueError(f"Could not find deserializer for type spec: {type_spec}")
    string = pathlib.Path(path).read_text()
    return deserializer(string)


# def load(
#     path: str,
#     type_spec: _structures.TypeSpecType,
#     cls: Optional[Type[T]] = None,
# ) -> T:
#     loader = get_deserializer_for_type_struct(type_spec)
#     return loader(path=path)
