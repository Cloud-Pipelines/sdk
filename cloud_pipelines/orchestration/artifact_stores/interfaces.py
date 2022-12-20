import abc
import os
import tempfile
from typing import Optional

from ..._components.components import _structures


__all__ = [
    "Artifact",
]


class Artifact(abc.ABC):
    def __init__(self, type_spec: Optional[_structures.TypeSpecType] = None):
        self._cached_bytes: Optional[bytes] = None
        self._type_spec = type_spec

    def download(self, path: Optional[str] = None) -> str:
        if not path:
            temp_dir = tempfile.mkdtemp()
            path = os.path.join(temp_dir, "data")
        self._download_to_path(path=path)
        return path

    @abc.abstractmethod
    def _download_to_path(self, path: str):
        raise NotImplementedError

    @abc.abstractmethod
    def _download_as_bytes(self) -> bytes:
        raise NotImplementedError

    # def read_text(self) -> str:
    #     if self._cached_bytes is None:
    #         self._cached_bytes = self._download_as_bytes()
    #     return self._cached_bytes.decode("utf-8")

    # def get_info(self) -> ArtifactInfo:
    #     return self._artifact_store.get_info(artifact_id=self._artifact_id)


# class ArtifactInfo:
#     is_dir: bool
#     size: int
#     hash: str
