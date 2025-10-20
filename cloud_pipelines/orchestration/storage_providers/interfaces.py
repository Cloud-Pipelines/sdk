import abc
import dataclasses
import hashlib
import json
import tempfile
from typing import Dict, Sequence, Type

__all__ = [
    "DataUri",
    "DataInfo",
    "UriReader",
    "UriWriter",
    "UriAccessor",
    "StorageProvider",
]


class DataUri(abc.ABC):
    _type_name_to_type_map: Dict[str, Type["DataUri"]] = {}
    _type_to_type_name_map: Dict[Type["DataUri"], str] = {}

    @classmethod
    @abc.abstractmethod
    def parse(cls, uri_string: str) -> "DataUri":
        raise NotImplementedError

    @abc.abstractmethod
    def join_path(self, relative_path: str) -> "DataUri":
        raise NotImplementedError

    @classmethod
    def _register_subclass(cls, type_name: str):
        if type_name in DataUri._type_name_to_type_map:
            raise ValueError(f"Type name {type_name} already exists")
        DataUri._type_name_to_type_map[type_name] = cls
        DataUri._type_to_type_name_map[cls] = type_name

    def to_dict(self) -> dict:
        cls = type(self)
        type_name = DataUri._type_to_type_name_map[cls]
        result = {
            "type": type_name,
            "properties": self.__dict__,
        }
        return result

    @staticmethod
    def from_dict(dict: dict) -> "DataUri":
        type_name = dict["type"]
        properties = dict["properties"]
        cls = DataUri._type_name_to_type_map.get(type_name)
        if not cls:
            raise ValueError(f"DataUri type {type_name} is not registered.")
        return cls(**properties)


@dataclasses.dataclass
class DataInfo:
    total_size: int
    is_dir: bool
    hashes: Dict[str, str]
    # Maybe add per-file info for directories:
    # file_info: name -> (size, hashes)


class _UriAccessorBase:
    def __init__(
        self,
        uri: DataUri,
        provider: "StorageProvider",
    ):
        self.uri = uri
        self._provider = provider


class UriReader(_UriAccessorBase):
    def download_to_path(self, path: str) -> None:
        self._provider.download(source_uri=self.uri, destination_path=path)

    def download_as_bytes(self) -> bytes:
        return self._provider.download_bytes(source_uri=self.uri)

    def download_as_text(self) -> str:
        return self._provider.download_bytes(source_uri=self.uri).decode("utf-8")

    def exists(self) -> bool:
        return self._provider.exists(uri=self.uri)

    def get_info(self) -> DataInfo:
        return self._provider.get_info(uri=self.uri)


class UriWriter(_UriAccessorBase):
    def upload_from_path(self, path: str) -> None:
        self._provider.upload(source_path=path, destination_uri=self.uri)

    def upload_from_bytes(self, data: bytes) -> None:
        self._provider.upload_bytes(data=data, destination_uri=self.uri)

    def upload_from_text(self, data: str) -> None:
        self._provider.upload_bytes(data=data.encode("utf-8"), destination_uri=self.uri)


# class UriAccessor(UriReader, UriWriter):
class UriAccessor(_UriAccessorBase):
    def make_subpath(self, relative_path: str) -> "UriAccessor":
        return UriAccessor(
            uri=self.uri.join_path(relative_path=relative_path),
            provider=self._provider,
        )

    def get_reader(self) -> "UriReader":
        return UriReader(
            uri=self.uri,
            provider=self._provider,
        )

    def get_writer(self) -> "UriWriter":
        return UriWriter(
            uri=self.uri,
            provider=self._provider,
        )


class StorageProvider(abc.ABC):
    @abc.abstractmethod
    def make_uri(self, *args, **kwargs) -> UriAccessor:
        raise NotImplementedError

    @abc.abstractmethod
    def parse_uri_get_accessor(self, uri_string: str) -> UriAccessor:
        raise NotImplementedError

    @abc.abstractmethod
    def upload(self, source_path: str, destination_uri: DataUri) -> None:
        raise NotImplementedError

    def upload_bytes(self, data: bytes, destination_uri: DataUri) -> None:
        with tempfile.NamedTemporaryFile("wb") as file:
            file.write(data)
            self.upload(source_path=file.name, destination_uri=destination_uri)

    @abc.abstractmethod
    def download(self, source_uri: DataUri, destination_path: str) -> None:
        raise NotImplementedError

    def download_bytes(self, source_uri: DataUri) -> bytes:
        with tempfile.NamedTemporaryFile() as file:
            self.download(source_uri=source_uri, destination_path=file.name)
            # Redundant?
            file.seek(0)
            data = file.read()
            return data

    # @abc.abstractmethod
    # def copy(self, source_uri: DataUri, destination_uri: DataUri):
    #     raise NotImplementedError

    # @abc.abstractmethod
    # def move(self, source_uri: DataUri, destination_uri: DataUri):
    #     raise NotImplementedError

    @abc.abstractmethod
    def exists(self, uri: DataUri) -> bool:
        raise NotImplementedError

    @abc.abstractmethod
    def get_info(self, uri: DataUri) -> DataInfo:
        raise NotImplementedError


@dataclasses.dataclass
class _FileInfo:
    path: str
    size: int
    hashes: Dict[str, str]


def _make_data_info_for_dir(file_info_list: Sequence[_FileInfo]) -> DataInfo:
    total_size = sum(file_info.size for file_info in file_info_list)
    hash_names = list(file_info_list[0].hashes.keys())
    # Stable sorting the files
    sorted_file_info_list = sorted(
        file_info_list, key=lambda info: info.path.encode("utf-8")
    )
    result_hashes = {}
    for hash_name in hash_names:
        # Structure that will be hashed
        file_info_dicts = [
            {
                "path": file_info.path,
                "size": file_info.size,
                "hash_name": hash_name.lower(),
                "hash": file_info.hashes[hash_name].lower(),
            }
            for file_info in sorted_file_info_list
        ]
        file_info_dicts_string = json.dumps(file_info_dicts)
        file_info_dicts_string_hash = hashlib.new(
            name=hash_name, data=file_info_dicts_string.encode("utf-8")
        ).hexdigest()
        result_hashes[hash_name.lower()] = file_info_dicts_string_hash
    return DataInfo(total_size=total_size, is_dir=True, hashes=result_hashes)
