import dataclasses
import hashlib
import logging
import os
import pathlib
import shutil
from typing import Dict, List

from . import interfaces

_LOGGER = logging.getLogger(name=__name__)

_DATA_HASHES = ["md5", "sha256"]


@dataclasses.dataclass
class LocalUri(interfaces.DataUri):
    path: str

    def join_path(self, relative_path: str) -> "LocalUri":
        new_path = os.path.join(self.path, relative_path)
        return LocalUri(path=new_path)


class LocalStorageProvider(interfaces.StorageProvider):
    def make_uri(self, path: str) -> interfaces.UriAccessor:
        return interfaces.UriAccessor(
            uri=LocalUri(path=path),
            provider=self,
        )

    def upload(self, source_path: str, destination_uri: LocalUri):
        destination_path = pathlib.Path(destination_uri.path)
        destination_path.parent.mkdir(parents=True, exist_ok=True)
        _LOGGER.debug(f"Downloading from {source_path} to {destination_path}")
        if not os.path.islink(source_path) and os.path.isdir(source_path):
            shutil.copytree(source_path, destination_path, symlinks=True)
        else:
            shutil.copy(source_path, destination_path, follow_symlinks=False)

    def upload_bytes(self, data: bytes, destination_uri: LocalUri):
        destination_path = pathlib.Path(destination_uri.path)
        destination_path.parent.mkdir(parents=True, exist_ok=True)
        _LOGGER.debug(f"Uploading data to {destination_path}")
        destination_path.write_bytes(data=data)

    def download(self, source_uri: LocalUri, destination_path: str):
        source_path = pathlib.Path(source_uri.path)
        _LOGGER.debug(f"Downloading from {source_path} to {destination_path}")
        os.makedirs(os.path.dirname(destination_path), exist_ok=True)
        if not source_path.is_symlink() and source_path.is_dir():
            shutil.copytree(src=source_path, dst=destination_path, symlinks=True)
        else:
            shutil.copy(src=source_path, dst=destination_path, follow_symlinks=False)

    def download_bytes(self, source_uri: LocalUri) -> bytes:
        source_path = pathlib.Path(source_uri.path)
        _LOGGER.debug(f"Downloading data from {source_path}")
        if source_path.is_symlink() or source_path.is_dir():
            raise RuntimeError(
                f"Path does not point to a file. Cannot read as bytes. {source_path}."
            )
        return source_path.read_bytes()

    def exists(self, uri: LocalUri) -> bool:
        return pathlib.Path(uri.path).exists()

    def get_info(self, uri: LocalUri) -> interfaces.DataInfo:
        return self._get_info_from_path(uri.path)

    def _get_info_from_path(self, path: str) -> interfaces.DataInfo:
        if not os.path.islink(path) and os.path.isdir(path):
            file_info_list = []
            empty_hashes = _calculate_empty_hashes(hash_names=_DATA_HASHES)
            for dir_path, _, file_names in os.walk(path, followlinks=False):
                # Handling the directories.
                # Directories (at least empty) need to be part of the hash.
                # Representing a directory as an empty object with path ending with slash
                # This is consistent with how AWS, Azure and GCS represent the directories.
                relative_dir_path = os.path.relpath(dir_path, path)
                # Directory entries end with slash
                canonical_dir_path = relative_dir_path.replace(os.path.sep, "/") + "/"
                if canonical_dir_path == "./":
                    # Should we skip the root directory entry or keep it?
                    # One small advantage of keeping it is that this can help us differentiate between an empty directory and nothing.
                    canonical_dir_path = ""
                dir_file_info = interfaces._FileInfo(
                    path=canonical_dir_path,
                    size=0,
                    hashes=empty_hashes,
                )
                file_info_list.append(dir_file_info)
                # Handling the files.
                for file_name in file_names:
                    full_file_path = os.path.join(dir_path, file_name)
                    relative_path = os.path.relpath(full_file_path, path)
                    canonical_path = relative_path.replace(os.path.sep, "/")
                    file_size = os.stat(full_file_path).st_size
                    hashes = _calculate_file_hashes(
                        path=full_file_path, hash_names=_DATA_HASHES
                    )
                    file_info = interfaces._FileInfo(
                        path=canonical_path,
                        size=file_size,
                        hashes=hashes,
                    )
                    file_info_list.append(file_info)
            data_info = interfaces._make_data_info_for_dir(file_info_list)
            data_info._file_info_list = file_info_list
            return data_info
        else:
            file_size = os.stat(path).st_size
            hashes = _calculate_file_hashes(path=path, hash_names=_DATA_HASHES)
            return interfaces.DataInfo(
                total_size=file_size, is_dir=False, hashes=hashes
            )


def _calculate_file_hashes(path: str, hash_names: List[str]) -> Dict[str, str]:
    hashers = {hash_name: hashlib.new(hash_name) for hash_name in hash_names}
    with open(path, "rb") as reader:
        while True:
            data = reader.read()
            if not data:
                break
            for hasher in hashers.values():
                hasher.update(data)
    hashes = {
        hash_name: hasher.hexdigest().lower() for hash_name, hasher in hashers.items()
    }
    return hashes


def _calculate_empty_hashes(hash_names: List[str]) -> Dict[str, str]:
    hashes = {
        hash_name: hashlib.new(hash_name).hexdigest().lower()
        for hash_name in hash_names
    }
    return hashes
