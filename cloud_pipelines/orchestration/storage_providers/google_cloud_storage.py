import dataclasses
import logging
import os
from typing import Optional

from google.cloud import storage

from . import interfaces

_LOGGER = logging.getLogger(name=__name__)


@dataclasses.dataclass
class GoogleCloudStorageUri(interfaces.DataUri):
    uri: str

    def join_path(self, relative_path: str) -> "GoogleCloudStorageUri":
        new_uri = self.uri.rstrip("/") + "/" + relative_path
        return GoogleCloudStorageUri(uri=new_uri)


class GoogleCloudStorageProvider(interfaces.StorageProvider):
    def __init__(self, client: Optional[storage.Client] = None) -> None:
        self._client = client or storage.Client()

    def make_uri(self, uri: str) -> interfaces.UriAccessor:
        # Validating the URI
        storage.Blob.from_string(uri)
        return interfaces.UriAccessor(
            uri=GoogleCloudStorageUri(uri=uri),
            provider=self,
        )

    def upload(self, source_path: str, destination_uri: GoogleCloudStorageUri):
        # TODO: Upload to temporary dir then rename
        destination_uri_str = destination_uri.uri
        _LOGGER.debug(f"Downloading from {source_path} to {destination_uri_str}")
        self._upload_to_uri(
            source_path=source_path, destination_uri=destination_uri_str
        )

    def _upload_file(self, source_file_path: str, destination_blob_uri: str):
        destination_blob = storage.Blob.from_string(
            uri=destination_blob_uri, client=self._client
        )
        destination_blob.upload_from_filename(filename=source_file_path, checksum="md5")

    def _upload_dir(self, source_dir_path: str, destination_dir_uri: str):
        for dir_entry_name in os.listdir(source_dir_path):
            source_path = os.path.join(source_dir_path, dir_entry_name)
            destination_uri = destination_dir_uri.rstrip("/") + "/" + dir_entry_name
            self._upload_to_uri(
                source_path=source_path, destination_uri=destination_uri
            )

    def _upload_to_uri(self, source_path: str, destination_uri: str):
        if not os.path.islink(source_path) and os.path.isdir(source_path):
            self._upload_dir(
                source_dir_path=source_path, destination_dir_uri=destination_uri
            )
        else:
            self._upload_file(
                source_file_path=source_path, destination_blob_uri=destination_uri
            )

    def upload_bytes(self, data: bytes, destination_uri: GoogleCloudStorageUri):
        destination_uri_str = destination_uri.uri
        destination_blob = storage.Blob.from_string(
            uri=destination_uri_str, client=self._client
        )
        _LOGGER.debug(f"Uploading data to {destination_uri_str}")
        destination_blob.upload_from_string(data=data, checksum="md5")

    def download(self, source_uri: GoogleCloudStorageUri, destination_path: str):
        source_uri_str = source_uri.uri
        os.makedirs(os.path.dirname(destination_path), exist_ok=True)
        _LOGGER.debug(f"Downloading from {source_uri_str} to {destination_path}")
        self._download_from_uri(
            source_uri=source_uri_str, destination_path=destination_path
        )

    def _download_from_uri(self, source_uri: str, destination_path: str):
        source_blob_or_dir = storage.Blob.from_string(
            uri=source_uri, client=self._client
        )
        if source_blob_or_dir.exists():
            os.makedirs(os.path.dirname(destination_path), exist_ok=True)
            source_blob_or_dir.download_to_filename(filename=destination_path)
        else:
            source_dir_prefix = source_blob_or_dir.name.rstrip("/") + "/"
            for source_blob in self._client.list_blobs(
                bucket_or_name=source_blob_or_dir.bucket, prefix=source_dir_prefix
            ):
                assert source_blob.name.startswith(source_dir_prefix)
                relative_source_blob_name = source_blob.name[len(source_dir_prefix) :]
                destination_file_path = os.path.join(
                    destination_path, relative_source_blob_name
                )
                os.makedirs(os.path.dirname(destination_file_path), exist_ok=True)
                source_blob.download_to_filename(filename=destination_file_path)

    def download_bytes(self, source_uri: GoogleCloudStorageUri) -> bytes:
        source_uri_str = source_uri.uri
        source_blob = storage.Blob.from_string(uri=source_uri_str, client=self._client)
        _LOGGER.debug(f"Downloading data from {source_uri_str}")
        return source_blob.download_as_bytes()
