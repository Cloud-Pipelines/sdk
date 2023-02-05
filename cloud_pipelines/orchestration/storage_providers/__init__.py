from .interfaces import *


def make_uri_accessor_from_uri(uri: str) -> UriAccessor:
    if "://" not in uri:
        from . import local_storage

        return local_storage.LocalStorageProvider().make_uri(path=uri)

    if uri.startswith("gs://"):
        from . import google_cloud_storage

        return google_cloud_storage.GoogleCloudStorageProvider().make_uri(uri=uri)
    else:
        raise ValueError(f"URI protocol is not supported: {uri}")
