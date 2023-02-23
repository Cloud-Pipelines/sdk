from . import _serializers


def save_pandas_dataframe_to_apache_parquet(obj: "pandas.DataFrame", path: str):
    obj.to_parquet(path=path)


def save_to_apache_parquet(obj, path: str):
    save_pandas_dataframe_to_apache_parquet(obj=obj, path=path)


def load_pandas_dataframe_from_apache_parquet(path: str) -> "pandas.DataFrame":
    import pandas

    return pandas.read_parquet(path=path)


def load_from_apache_parquet(path: str):
    return load_pandas_dataframe_from_apache_parquet(path=path)


_serializers.python_type_name_to_type_spec["pandas.DataFrame"] = "ApacheParquet"
_serializers.savers["ApacheParquet"] = save_to_apache_parquet
_serializers.loaders["ApacheParquet"] = load_from_apache_parquet


def save_as_TensorflowSavedModel(obj, path: str):
    import tensorflow

    try:
        tensorflow.keras.models.save_model(model=obj, filepath=path)
    except:
        tensorflow.saved_model.save(obj=obj, export_dir=path)


def load_from_TensorflowSavedModel(path: str):
    import tensorflow

    try:
        return tensorflow.keras.models.load_model(filepath=path)
    except:
        return tensorflow.saved_model.load(export_dir=path)


def save_as_PyTorchScriptModule(obj, path: str):
    from torch import jit

    jit.script(obj).save(path)


def load_from_PyTorchScriptModule(path: str):
    from torch import jit

    return jit.load(path)


_serializers.python_type_name_to_type_spec[
    "tensorflow.python.training.tracking.base.Trackable"
] = "TensorflowSavedModel"
_serializers.savers["TensorflowSavedModel"] = save_as_TensorflowSavedModel
_serializers.loaders["TensorflowSavedModel"] = load_from_TensorflowSavedModel

_serializers.python_type_name_to_type_spec[
    "torch.nn.modules.module.Module"
] = "PyTorchScriptModule"
_serializers.savers["PyTorchScriptModule"] = save_as_PyTorchScriptModule
_serializers.loaders["PyTorchScriptModule"] = load_from_PyTorchScriptModule
