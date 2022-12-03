import abc
from typing import Optional


__all__ = [
    "Artifact",
]


class Artifact(abc.ABC):
    def __init__(self):
        self._cached_bytes: Optional[bytes] = None

    @abc.abstractmethod
    def download(self, path: str):
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
