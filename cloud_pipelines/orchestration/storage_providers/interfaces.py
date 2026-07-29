import abc
import dataclasses
import hashlib
import json
import logging
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

_DEFAULT_HASH_ALGO = "sha256"

_logger = logging.getLogger(__name__)


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
    if not file_info_list:
        return DataInfo(total_size=0, is_dir=True, hashes={})

    total_size = sum(file_info.size for file_info in file_info_list)

    # We want to compute directory hashes based on the file hashes.
    # Each FileInfo entry may have multiple hashes.
    # Some hashes might be missing on some files. For example, GCS composite objects lack MD5 hash.
    # We need hashes of all files to calculate the directory hash.
    # So we have to be careful not to use hashes that do not have full coverage.
    hash_counts: dict[str, int] = (
        {}
    )  # Not using `set` since I want to preserve the hash order.
    for file_info in file_info_list:
        for hash_name in file_info.hashes:
            hash_counts[hash_name] = hash_counts.get(hash_name, 0) + 1

    hashes_with_full_coverage = [
        hash_name
        for hash_name, hash_count in hash_counts.items()
        if hash_count == len(file_info_list)
    ]
    if len(hashes_with_full_coverage) != len(hash_counts):
        _logger.warning(
            f"_make_data_info_for_dir: Some hashes do are not present for all {len(file_info_list)} files: {hash_counts=}. {hashes_with_full_coverage=}"
        )

    # Stable sorting the files
    sorted_file_info_list = sorted(
        file_info_list, key=lambda info: info.path.encode("utf-8")
    )
    result_hashes = {}
    for hash_name in hashes_with_full_coverage:
        # Some hashes like "crc32" are not available from hashlib. Replacing them with a supported algorithm.
        # Actually, we do not need to calculate directory hash using same hashing algorithm. We can use same algorithm for everything, but that would be a breaking change.
        # So, for now we only do this for hashes that are not available in hashlib, resulting in directory hashes like "crc32c-dir-sha256".
        # Maybe in the future we can standardize on the new directory hash names while keeping backwards compatible results (return multiple hashes).
        if hash_name in hashlib.algorithms_available:
            dir_hash_name = hash_name
            hash_algo = hash_name
        else:
            dir_hash_name = f"{hash_name}-dir-{_DEFAULT_HASH_ALGO}"
            hash_algo = _DEFAULT_HASH_ALGO

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
            name=hash_algo, data=file_info_dicts_string.encode("utf-8")
        ).hexdigest()
        result_hashes[dir_hash_name.lower()] = file_info_dicts_string_hash
    return DataInfo(total_size=total_size, is_dir=True, hashes=result_hashes)
