import base64
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


GoogleCloudStorageUri._register_subclass("google_cloud_storage")


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
        _LOGGER.debug(f"Uploading from {source_path} to {destination_uri_str}")
        self._upload_to_uri(
            source_path=source_path, destination_uri=destination_uri_str
        )

    def _upload_file(self, source_file_path: str, destination_blob_uri: str):
        destination_blob = storage.Blob.from_string(
            uri=destination_blob_uri, client=self._client
        )
        destination_blob.upload_from_filename(filename=source_file_path, checksum="md5")

    def _upload_dir(self, source_dir_path: str, destination_dir_uri: str):
        # Creating the directory object (zero-byte object with name ending in slash)
        storage.Blob.from_string(
            uri=destination_dir_uri.rstrip("/") + "/", client=self._client
        ).upload_from_string(data="", checksum="md5")

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
                if source_blob.name.endswith("/"):
                    # It's a zero-size object that represents a directory
                    assert source_blob.size == 0
                    os.makedirs(destination_file_path, exist_ok=True)
                else:
                    os.makedirs(os.path.dirname(destination_file_path), exist_ok=True)
                    source_blob.download_to_filename(filename=destination_file_path)

    def download_bytes(self, source_uri: GoogleCloudStorageUri) -> bytes:
        source_uri_str = source_uri.uri
        source_blob = storage.Blob.from_string(uri=source_uri_str, client=self._client)
        _LOGGER.debug(f"Downloading data from {source_uri_str}")
        return source_blob.download_as_bytes()

    def exists(self, uri: GoogleCloudStorageUri) -> bool:
        blob_uri = uri.uri.rstrip("/")
        file_blob = storage.Blob.from_string(uri=blob_uri, client=self._client)
        # The "directory objects" are expected to exist for directories
        dir_blob = storage.Blob.from_string(uri=blob_uri + "/", client=self._client)
        return file_blob.exists() or dir_blob.exists()

    def get_info(self, uri: GoogleCloudStorageUri) -> interfaces.DataInfo:
        return self._get_info_from_uri(uri.uri)

    def _get_info_from_uri(self, uri: str) -> interfaces.DataInfo:
        file_info_list = []
        blob_or_dir = storage.Blob.from_string(uri=uri, client=self._client)
        if blob_or_dir.exists():
            blob = blob_or_dir
            blob.reload()
            return interfaces.DataInfo(
                total_size=blob.size,
                is_dir=False,
                # blob.md5_hash is a base64-encoded hash digest byte array. E.g. "1B2M2Y8AsgTpgAmY7PhCfg=="
                hashes={"md5": base64.decodebytes(blob.md5_hash.encode("ascii")).hex()},
            )
        else:
            # Note: Each empty dir root is represented as a 0-byte object with trailing slash:
            # _FileInfo(path='', size=0, hashes={'md5': 'd41d8cd98f00b204e9800998ecf8427e'}),
            # _FileInfo(path='subdir/', size=0, hashes={'md5': 'd41d8cd98f00b204e9800998ecf8427e'}),
            dir_prefix = blob_or_dir.name.rstrip("/") + "/"
            for blob in self._client.list_blobs(
                bucket_or_name=blob_or_dir.bucket, prefix=dir_prefix
            ):
                blob.reload()
                assert blob.name.startswith(dir_prefix)
                relative_source_blob_name = blob.name[len(dir_prefix) :]
                file_info_list.append(
                    interfaces._FileInfo(
                        path=relative_source_blob_name,
                        size=blob.size,
                        hashes={
                            # blob.md5_hash is a base64-encoded hash digest byte array. E.g. "1B2M2Y8AsgTpgAmY7PhCfg=="
                            "md5": base64.decodebytes(
                                blob.md5_hash.encode("ascii")
                            ).hex()
                        },
                    )
                )
            data_info = interfaces._make_data_info_for_dir(file_info_list)
            data_info._file_info_list = file_info_list
            return data_info
