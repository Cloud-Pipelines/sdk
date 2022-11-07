import abc
from typing import Optional


__all__ = [
    "ArtifactStore",
    "Artifact",
]


class Artifact:
    def __init__(self, id: str, artifact_store: "ArtifactStore"):
        self._id = id
        self._artifact_store = artifact_store
        self._cached_bytes: Optional[bytes] = None

    def download(self, path: str):
        self._artifact_store._download_impl(artifact_id=self._id, destination_path=path)

    def read_text(self) -> str:
        if self._cached_bytes is None:
            self._cached_bytes = self._artifact_store._download_bytes_impl(
                artifact_id=self._id
            )
        return self._cached_bytes.decode("utf-8")

    # def get_info(self) -> ArtifactInfo:
    #     return self._artifact_store.get_info(artifact_id=self._artifact_id)


# TODO: Switch to typing.Protocol when adopting Python 3.8
class ArtifactStore(abc.ABC):
    def upload(self, path: str) -> Artifact:
        artifact_id = self._upload_impl(source_path=path)
        return Artifact(id=artifact_id, artifact_store=self)

    def upload_text(self, text: str) -> Artifact:
        artifact_id = self._upload_bytes_impl(data=text.encode("utf-8"))
        return Artifact(id=artifact_id, artifact_store=self)

    def get_artifact_by_id(self, artifact_id: str):
        return Artifact(id=artifact_id, artifact_store=self)

    @abc.abstractmethod
    def _download_impl(self, artifact_id: str, destination_path: str):
        raise NotImplementedError

    @abc.abstractmethod
    def _download_bytes_impl(self, artifact_id: str) -> bytes:
        raise NotImplementedError

    @abc.abstractmethod
    def _upload_impl(self, source_path: str) -> str:
        raise NotImplementedError

    @abc.abstractmethod
    def _upload_bytes_impl(self, data: bytes) -> str:
        raise NotImplementedError

    # @abc.abstractmethod
    # def get_info(self, artifact_id: str) -> ArtifactInfo:
    #     raise NotImplementedError()


# class ArtifactInfo:
#     is_dir: bool
#     size: int
#     hash: str
