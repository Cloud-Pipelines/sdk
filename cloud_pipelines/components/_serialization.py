import pathlib
from typing import Any, Optional
import warnings

from .._components.components import _data_passing as _internal_data_passing
from .._components.components import _structures


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
            type_spec = _get_full_class_name(obj)
        warnings.warn(
            f'Missing type spec was inferred as "{type_spec}" based on the object "{obj}".'
        )
    type_name = _type_spec_to_type_name(type_spec)

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
