""" convience methods for creating path management

License:
    BSD, see LICENSE.md
"""
from typing import Optional
from spectral_trend_database.config import config as c


#
# METHODS
#
def local(
        *args: str,
        root_dir: Optional[str] = c.ROOT_DIR,
        local_dir: Optional[str] = c.LOCAL_DATA_DIR) -> str:
    parts = [root_dir, local_dir] + list(args)
    return '/'.join([p for p in parts if p])


def gcs(
        *args: str,
        bucket: Optional[str] = c.GCS_BUCKET,
        folder: Optional[str] = c.GCS_ROOT_FOLDER,
        as_uri: bool = True,
        as_url: bool = False) -> str:
    path = local(*args, root_dir=bucket, local_dir=folder)
    if as_uri:
        path = f'{c.URI_PREFIX}{path}'
    elif as_url:
        path = f'{c.URL_PREFIX}{path}'
    return path
