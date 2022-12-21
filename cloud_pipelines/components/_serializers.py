def _serialize_str(str_value: str) -> str:
    if not isinstance(str_value, str):
        raise TypeError(
            'Value "{}" has type "{}" instead of str.'.format(
                str(str_value), str(type(str_value))
            )
        )
    return str_value


_deserialize_string = str


def _serialize_int(int_value: int) -> str:
    if isinstance(int_value, str):
        return int_value
    if not isinstance(int_value, int):
        raise TypeError(
            'Value "{}" has type "{}" instead of int.'.format(
                str(int_value), str(type(int_value))
            )
        )
    return str(int_value)


_deserialize_integer = int


def _serialize_float(float_value: float) -> str:
    if isinstance(float_value, str):
        return float_value
    if not isinstance(float_value, (float, int)):
        raise TypeError(
            'Value "{}" has type "{}" instead of float.'.format(
                str(float_value), str(type(float_value))
            )
        )
    return str(float_value)


_deserialize_float = float


def _serialize_bool(bool_value: bool) -> str:
    if isinstance(bool_value, str):
        return bool_value
    if not isinstance(bool_value, bool):
        raise TypeError(
            'Value "{}" has type "{}" instead of bool.'.format(
                str(bool_value), str(type(bool_value))
            )
        )
    return str(bool_value)


def _deserialize_boolean(string: str) -> bool:
    string = string.lower()
    if string == "true":
        return True
    elif string == "false":
        return False
    else:
        raise ValueError(f"Invalid serialized boolean value: {string}")


def _serialize_to_json(obj) -> str:
    if isinstance(obj, str):
        return obj
    import json

    def default_serializer(obj):
        if hasattr(obj, "to_struct"):
            return obj.to_struct()
        elif hasattr(obj, "to_dict"):
            return obj.to_dict()
        else:
            raise TypeError(
                "Object of type '%s' is not JSON serializable and does not have .to_struct() method."
                % obj.__class__.__name__
            )

    return json.dumps(obj, default=default_serializer, sort_keys=True)


_serialize_list = _serialize_to_json
_serialize_dict = _serialize_to_json


def _deserialize_json_array(string: str) -> list:
    import json

    obj = json.loads(string)
    if not isinstance(obj, list):
        raise ValueError(
            f'The loaded object has type "{type(obj).__name__}" instead of list. Object: {obj}'
        )
    return obj


def _deserialize_json_object(string: str) -> dict:
    import json

    obj = json.loads(string)
    if not isinstance(obj, dict):
        raise ValueError(
            f'The loaded object has type "{type(obj).__name__}" instead of dict. Object: {obj}'
        )
    return obj


_serializers = {}
# TODO: Attach type specs to the serializers
_serializers[str] = _serialize_str  # String
_serializers[int] = _serialize_int  # Integer
_serializers[float] = _serialize_float  # Float
_serializers[bool] = _serialize_bool  # Boolean
_serializers[list] = _serialize_list  # JsonArray
_serializers[dict] = _serialize_dict  # JsonObject


_deserializers = {}
_deserializers["String"] = _deserialize_string
_deserializers["Integer"] = _deserialize_integer
_deserializers["Float"] = _deserialize_float
_deserializers["Boolean"] = _deserialize_boolean
_deserializers["JsonArray"] = _deserialize_json_array
_deserializers["JsonObject"] = _deserialize_json_object

_python_type_mappers = []
# Converting generic type aliases: typing.List -> list, typing.Dict -> dict
_python_type_mappers.append(lambda typ: getattr(type, "__origin__", typ))

# These non-canonical aliases should not be used
# TODO: Show warning when encountering any of these aliases
# TODO: Check whether any components use these aliases.
_alias_type_name_to_canonical = {}
_alias_type_name_to_canonical["str"] = "String"
_alias_type_name_to_canonical["int"] = "Integer"
_alias_type_name_to_canonical["float"] = "Float"
_alias_type_name_to_canonical["Bool"] = "Boolean"
_alias_type_name_to_canonical["bool"] = "Boolean"
# _alias_type_name_to_canonical["typing.List"] = "JsonArray"
_alias_type_name_to_canonical["List"] = "JsonArray"
_alias_type_name_to_canonical["list"] = "JsonArray"
_alias_type_name_to_canonical["Dictionary"] = "JsonObject"
# _alias_type_name_to_canonical["typing.Dict"] = "JsonArray"
_alias_type_name_to_canonical["Dict"] = "JsonObject"
_alias_type_name_to_canonical["dict"] = "JsonObject"
