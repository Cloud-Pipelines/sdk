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
