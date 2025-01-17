""" convience methods for bigquery

License:
    BSD, see LICENSE.md
"""
from typing import Optional, Union
import re
from pathlib import Path
import pandas as pd
from google.cloud import storage  # type: ignore
from google.cloud import bigquery as bq
from spectral_trend_database.config import config as c
from spectral_trend_database import utils


#
# CONSTANTS
#
DATE_FMT = '%Y-%m-%d'


#
# CLOUD STORAGE
#
def process_gcs_path(
        path: str,
        *args: str,
        prefix: str = c.URI_PREFIX,
        bucket: Optional[str] = None) -> tuple[str, str]:
    """ process gcs path

    Converts path string, or path string plus additional args, to a
    cloud storage bucket, filepath tuple

    Usage:

    ```python
    process_gcs_path('gs://a/b/c.json') -> 'a', 'b/c.json'
    process_gcs_path('a/b/c.json') -> 'a', 'b/c.json'
    process_gcs_path('gs://a', 'b', 'c.json') -> 'a', 'b/c.json'
    process_gcs_path('b', 'c.json', bucket='a') -> 'a', 'b/c.json'
    ```

    Args:

        path (str): file-path may or may not include scheme
        *args (str): ordered additional parts to path (see Usage above)
        prefix (str = c.URI_PREFIX):
            if exists strip prefix from begining of string. For
            URIs use `c.URI_PREFIX`, for URLs us `c.URL_PREFIX`.

    Returns:

        (tuple) name of gcs bucket, gcs filename-prefix
    """
    if prefix:
        path = re.sub(f'^{prefix}', '', path)
    parts = path.split('/')
    if not bucket:
        bucket = parts[0]
        parts = parts[1:]
    parts = parts + list(args)
    if bucket:
        path = '/'.join(parts)
        return bucket, path
    else:
        raise ValueError('spectral_trend_database.gcp.process_gcs_path: path is empty')


def gcs_list(
        path: str,
        *args: str,
        bucket: Optional[str] = None,
        search: Optional[str] = None,
        prefix: Optional[str] = c.URI_PREFIX,
        project: Optional[str] = None,
        client: Optional[storage.Client] = None) -> list[str]:
    """ list cloud storage objects

    Args:

        path (str): file-path may or may not include scheme
        *args (str): ordered additional parts to path (see Usage in `process_gcs_path` above)
        search (Optional[str] = None): only return paths containing <search>
        prefix (str = c.URI_PREFIX):
            if exists add prefix to begining of string. For
            URIs use `c.URI_PREFIX`, for URLs us `c.URL_PREFIX`.
        project (Optional[str] = None): gcp project name
        client Optional[bq.Client] = None):
            instance of bigquery client
            if None a new one will be instantiated

    Returns:

        list of cloud storage object paths
    """
    if client is None:
        client = storage.Client(project=project)
    bucket, path = process_gcs_path(path, *args, bucket=bucket)
    blobs = client.list_blobs(bucket, prefix=path)
    paths = [f'{bucket}/{b.name}' for b in blobs]
    if search:
        paths = [p for p in paths if re.search(search, p)]
    if prefix:
        paths = [f'{prefix}{p}' for p in paths]
    return paths


def gcs_list_folders(
        path: str,
        *args: str,
        bucket: Optional[str] = None,
        search: Optional[str] = None,
        project: Optional[str] = None,
        client: Optional[storage.Client] = None,
        include_bucket: bool = True,
        include_gs: bool = False) -> list[str]:
    """ list cloud storage objects

    Args:

        path (str): file-path may or may not include scheme
        *args (str): ordered additional parts to path (see Usage in `process_gcs_path` above)
        search (Optional[str] = None): only return folders containing <search>
        project (Optional[str] = None): gcp project name
        client Optional[bq.Client] = None):
            instance of bigquery client
            if None a new one will be instantiated
        include_bucket (bool = True): if true prefix with bucket-name
        include_gs (bool = False): if true prefix with gs://

    Returns:

        list of cloud storage object paths
    """
    if client is None:
        client = storage.Client(project=project)
    bucket, path = process_gcs_path(path, *args, bucket=bucket)
    path_count = path.count('/')
    blobs = client.list_blobs(bucket, prefix=path)
    folders = list(set(
        '/'.join(b.name.split('/')[:path_count + 2])
        for b in blobs
        if b.name.count('/') > path_count + 1))
    if search:
        folders = [p for p in folders if re.search(search, p)]
    if include_bucket:
        folders = [f'{bucket}/{p}' for p in folders]
    if include_gs:
        if not include_bucket:
            err = (
                'gcp.gcs_list_folders: '
                'if `include_gs` is True `include_bucket` must also be True')
            raise ValueError(err)
        folders = [f'gs://{p}' for p in folders]
    return folders


def upload_file(
        src: str,
        path: str,
        *args: str,
        bucket_name: Optional[str] = None,
        search: Optional[str] = None,
        prefix: Optional[str] = c.URI_PREFIX,
        project: Optional[str] = None,
        client: Optional[storage.Client] = None) -> str:
    """ upload file to cloud storage

    Args:

        src: (str): local source file-path
        path (str): destination gcp file-path may or may not include scheme
        *args (str): ordered additional parts to path (see Usage in `process_gcs_path` above)
        search (Optional[str] = None): only return paths containing <search>
        prefix (str = c.URI_PREFIX):
            if exists add prefix to begining of string. For
            URIs use `c.URI_PREFIX`, for URLs us `c.URL_PREFIX`.
        project (Optional[str] = None): gcp project name
        client Optional[bq.Client] = None):
            instance of bigquery client
            if None a new one will be instantiated

    Returns:

        (str) destination uri of uploaded object
    """
    if client is None:
        client = storage.Client(project=project)
    bucket_name, path = process_gcs_path(path, *args, bucket=bucket_name)
    dest_uri = f'{c.URI_PREFIX}{bucket_name}/{path}'
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(path)
    blob.upload_from_filename(src)
    return dest_uri


#
# BIG QUERY
#
def load_or_create_dataset(
        name: str,
        location: str = c.DEFAULT_LOCATION,
        project: Optional[str] = None,
        client: Optional[bq.Client] = None,
        timeout: int = c.DEFAULT_TIMEOUT,
        warn: bool = False) -> bq.Dataset:
    """ load or create bigquery dataset

    Args:

        name (str): name of dataset
        location (str='US'): gcp location
        project (Optional[str] = None): gcp project name
        client Optional[bq.Client] = None):
            instance of bigquery client
            if None a new one will be instantiated
        timeout (int=30): timeout (seconds)
        warn (bool = False): print message if dataset exist

    Returns:

        bigquery dataset
    """
    if client is None:
        client = bq.Client(project=project)
    try:
        dataset = client.get_dataset(name)
        if warn:
            msg = f'DATASET {name} EXSITS'
            utils.message(msg, 'gcp', 'load_or_create_dataset')
    except:
        dataset_id = f'{client.project}.{name}'
        dataset = bq.Dataset(dataset_id)
        dataset.location = location
        dataset = client.create_dataset(dataset, timeout=timeout)
        msg = "Created dataset {}.{}".format(client.project, dataset.dataset_id)
        utils.message(msg, 'gcp', 'load_or_create_dataset')
    return dataset


def create_or_update_table_from_json(
        dataset: bq.Dataset,
        name: str,
        uri: str,
        wait: bool = True,
        return_table: bool = False,
        project: Optional[str] = None,
        client: Optional[bq.Client] = None,
        timeout: int = c.DEFAULT_TIMEOUT) -> Union[bq.Table, None]:
    """ create/update table from (line deliminated) json

    Note:

    list valued columns failed if they contained NaNs. Substitue NaNs
    with value (such as -99999) and replace after loading table.

    Args:

        dataset (bq.Dataset): instance of bigquery dataset to create table in
        name (str): name for table
        uri (str): cloud-storage uri of line-deliminated-json
        wait (bool = True): wait from creation to complete before continuing
        return_table (bool = False): if true return bigquery table instance
        project (Optional[str] = None): gcp project name
        client Optional[bq.Client] = None):
            instance of bigquery client
            if None a new one will be instantiated
        timeout (int=30): timeout (seconds)

    Returns:

        if <return table>: bigquery table instance
    """
    if client is None:
        client = bq.Client(project=project)
    table_id = f'{client.project}.{dataset.dataset_id}.{name}'
    job_config = bq.LoadJobConfig(
        autodetect=True,
        source_format=bq.SourceFormat.NEWLINE_DELIMITED_JSON,
    )
    job = client.load_table_from_uri(
        uri,
        table_id,
        location=client.location,
        job_config=job_config)
    if wait:
        job.result()
    if return_table:
        return client.get_table(table_id)
    else:
        return None
