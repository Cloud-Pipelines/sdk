import logging
import os
import pathlib
import shutil
import uuid

from . import interfaces

_LOGGER = logging.getLogger(name=__name__)
_LOGGER.setLevel("DEBUG")


class LocalArtifactStore(interfaces.ArtifactStore):
    def __init__(self, root_dir: str):
        self._root_dir = root_dir

    def _download_impl(self, artifact_id: str, destination_path: str):
        source_path = pathlib.Path(self._root_dir) / artifact_id
        _LOGGER.info(f"Downloading from {source_path} to {destination_path}")
        if not source_path.is_symlink() and source_path.is_dir():
            shutil.copytree(src=source_path, dst=destination_path, symlinks=True)
        else:
            shutil.copy(src=source_path, dst=destination_path, follow_symlinks=False)

    def _download_bytes_impl(self, artifact_id: str) -> bytes:
        source_path = pathlib.Path(self._root_dir) / artifact_id
        _LOGGER.info(f"Downloading data from {source_path}")
        if source_path.is_symlink() or source_path.is_dir():
            raise RuntimeError(
                f"Artifact is not a file. Cannot read as bytes. {artifact_id}."
            )
        return source_path.read_bytes()

    def _generate_artifact_id(self) -> str:
        return "uuid4=" + str(uuid.uuid4())

    def _upload_impl(self, source_path: str) -> str:
        artifact_id = self._generate_artifact_id()
        dest_path = pathlib.Path(self._root_dir) / artifact_id
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        _LOGGER.info(f"Uploading from {source_path} to {dest_path}")
        if not os.path.islink(source_path) and os.path.isdir(source_path):
            shutil.copytree(source_path, dest_path, symlinks=True)
        else:
            shutil.copy(source_path, dest_path, follow_symlinks=False)
        return artifact_id

    def _upload_bytes_impl(self, data: bytes) -> str:
        artifact_id = self._generate_artifact_id()
        dest_path = pathlib.Path(self._root_dir) / artifact_id
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        _LOGGER.info(f"Uploading data to {dest_path}")
        dest_path.write_bytes(data=data)
        return artifact_id
