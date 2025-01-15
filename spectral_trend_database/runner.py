""" methods for running scripts

License:
    BSD, see LICENSE.md
"""
from typing import Optional, Union, Sequence, Callable
from pathlib import Path
from IPython.display import display
import json
import pandas as pd
import mproc  # type: ignore[import-untyped]
from spectral_trend_database.config import config as c
from spectral_trend_database import utils
from spectral_trend_database import paths
from spectral_trend_database import gcp
from spectral_trend_database import types


#
# CONSTANTS
#
DEFAULT_MAP_METHOD: types.MAP_METHOD = 'threadpool'
NOISY = True
DRY_RUN = False
EXT = 'json'


#
# HELPERS
#
def post_process_row(row: dict, list_vars: list[str]) -> dict:
    row[c.DATE_COLUMN] = list(utils.cast_duck_array(row[c.DATE_COLUMN]))
    for key in list_vars:
        row[key] = list(row[key])
    return row


def mapper(func: Union[types.MAP_METHOD, Callable]) -> Callable:
    if func == 'sequential':
        mapper = mproc.map_sequential
    elif func == 'threadpool':
        mapper = mproc.map_with_threadpool
    elif func == 'pool':
        mapper = mproc.map_with_pool
    elif isinstance(func, str):
        err = (
            'spectral_trend_database.runner.get_mapper: '
            f'invalid map function name ({func}) must be callable '
            f'or one of {types.MAP_METHOD_ARGS}.'
        )
        raise ValueError(err)
    return mapper


def get_paths(
        name: str,
        local_folder: Optional[str] = None,
        gcs_folder: Optional[str] = None,
        *args: types.PATH_PARTS) -> tuple[str, str]:
    for v in args:
        name += f'-{v}'
    local_dest = paths.local(
        local_folder,
        name,
        ext=EXT)
    gcs_dest = paths.gcs(
        gcs_folder,
        name,
        ext=EXT)
    return local_dest, gcs_dest


#
# MAIN
#
def destination_strings(
        *args: types.PATH_PARTS,
        table_name: Optional[str] = None,
        local_folder: Optional[str] = None,
        gcs_folder: Optional[str] = None,
        dataset_name: Optional[str] = None,
        file_base_name: Optional[str] = None,
        file_ext: Optional[str] = EXT,
        dry_run: bool = DRY_RUN,
        noisy: bool = NOISY) -> tuple[
            Union[str, None],
            Union[str, None],
            Union[str, None],
            Union[str, None]]:
    if table_name:
        if not dataset_name:
            dataset_name, table_name = table_name.split('.', maxsplit=1)
        table_name = table_name.upper()
        if not file_base_name:
            file_base_name = table_name.lower()
    assert isinstance(file_base_name, str)
    local_dest, gcs_dest = get_paths(
        file_base_name,
        local_folder,
        gcs_folder,
        *args)
    if not dry_run:
        Path(local_dest).parent.mkdir(parents=True, exist_ok=True)
    if noisy:
        for arg in args:
            print('-', arg)
        print('- destination_strings:')
        print('\t dataset_name:', dataset_name)
        print('\t table_name:', table_name)
        print('\t local_dest:', local_dest)
        print('\t gcs_dest:', gcs_dest)
    return table_name, dataset_name, local_dest, gcs_dest


def table_name_and_paths(
        *path_parts: str,
        table_name: Optional[str] = None,
        year: Optional[int] = None,
        file_name: Optional[str] = None,
        ext: Optional[str] = 'json'):
    if table_name:
        table_name = table_name.upper()
    if not file_name:
        file_name = table_name.lower()
    if year is not None:
        file_name = f'{file_name}-{year}'
    if ext:
        file_name = f'{file_name}.{ext}'
    local_dest = paths.local(
        *path_parts,
        file_name)
    gcs_dest = paths.gcs(
        *path_parts,
        file_name)
    return table_name, local_dest, gcs_dest


def print_errors(errors: list[str]):
    errors = [e for e in errors if e]
    if errors:
        df = pd.DataFrame(errors)
        try:
            _df = df[~df.error.isna()]
            if _df.shape[0]:
                print(f'ERRORS [{_df.shape[0]}]:')
                display(_df.groupby('error').size())
        except:
            pass
        try:
            _df = df[~df.warning.isna()]
            if _df.shape[0]:
                print(f'WARNING [{_df.shape[0]}]:')
                display(_df.groupby('warning').size())
        except:
            pass


def save_to_gcp(
        src: str,
        gcs_dest: Optional[str],
        dataset_name: Optional[str],
        table_name: Optional[str],
        bigquery_loc: str = c.LOCATION,
        gcs_uri: Optional[str] = None,
        remove_src: bool = False,
        noisy: bool = NOISY,
        dry_run: bool = False) -> None:
    if gcs_dest:
        assert isinstance(gcs_dest, str)
        if dry_run:
            if noisy:
                print('- dry_run [gcs]:', gcs_dest)
        else:
            gcs_uri = gcp.upload_file(src, gcs_dest)
            if noisy:
                print('- uploaded to gcs:', gcs_uri)
            if gcs_uri and remove_src:
                Path(src).unlink()
    if table_name:
        if dry_run:
            if noisy:
                print(f'- dry_run: update table [{dataset_name}.{table_name}]')
        else:
            if noisy:
                print(f'- update table [{dataset_name}.{table_name}]')
            assert isinstance(dataset_name, str)
            assert isinstance(gcs_uri, str)
            gcp.create_or_update_table_from_json(
                gcp.load_or_create_dataset(dataset_name, bigquery_loc),
                name=table_name,
                uri=gcs_uri)
