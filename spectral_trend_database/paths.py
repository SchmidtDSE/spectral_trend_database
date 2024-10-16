""" convience methods for creating path management

License:
    BSD, see LICENSE.md
"""
from typing import Optional
import secrets
from spectral_trend_database.config import config as c
from spectral_trend_database import types


#
# CONSTANTS
#
URI = 'uri'
URL = 'url'


#
# METHODS
#
def local(
        *args: types.PATH_PARTS,
        root_dir: Optional[str] = c.ROOT_DIR,
        local_dir: Optional[str] = c.LOCAL_DATA_DIR,
        ext: Optional[str] = None) -> str:
    parts = [root_dir, local_dir] + list(args)
    path = '/'.join([str(p) for p in parts if p])
    if ext:
        path = f'{path}.{ext}'
    return path


def gcs(
        *args: types.PATH_PARTS,
        bucket: Optional[str] = c.GCS_BUCKET,
        folder: Optional[str] = c.GCS_ROOT_FOLDER,
        prefix: Optional[str] = URI,
        kill_cache: bool = False,
        ext: Optional[str] = None) -> str:
    path = local(*args, root_dir=bucket, local_dir=folder)
    if prefix:
        if prefix == URI:
            prefix = c.URI_PREFIX
        elif prefix == URL:
            prefix = c.URL_PREFIX
        path = f'{prefix}{path}'
    if ext:
        path = f'{path}.{ext}'
    if kill_cache:
        path = f'{path}?kill_cache={secrets.token_urlsafe(16)}'
    return path
