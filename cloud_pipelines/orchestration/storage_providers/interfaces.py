import abc
import tempfile

__all__ = ["DataUri", "UriReader", "UriWriter", "UriAccessor", "StorageProvider"]


class DataUri(abc.ABC):
    @abc.abstractmethod
    def join_path(self, relative_path: str) -> "DataUri":
        raise NotImplementedError


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


class UriWriter(_UriAccessorBase):
    def upload_from_path(self, path: str) -> None:
        self._provider.upload(source_path=path, destination_uri=self.uri)

    def upload_from_bytes(self, data: bytes) -> None:
        self._provider.upload_bytes(data=data, destination_uri=self.uri)


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
    def make_uri(self, **kwargs) -> UriAccessor:
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
