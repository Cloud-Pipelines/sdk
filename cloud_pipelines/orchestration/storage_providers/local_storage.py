import dataclasses
import logging
import os
import pathlib
import shutil

from . import interfaces

_LOGGER = logging.getLogger(name=__name__)


@dataclasses.dataclass
class LocalUri(interfaces.DataUri):
    path: str

    def join_path(self, relative_path: str) -> "LocalUri":
        new_path = os.path.join(self.path, relative_path)
        return LocalUri(path=new_path)


class LocalStorageProvider(interfaces.StorageProvider):
    def make_uri(
        self, path: str
    ) -> interfaces.UriAccessor:
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
