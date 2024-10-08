""" COMPUTE RAW SPECTRAL INCIDES

authors:
    - name: Brookie Guzder-Williams

affiliations:
    - University of California Berkeley,
      The Eric and Wendy Schmidt Center for Data Science & Environment

steps:

    1. ...

outputs:


runtime: ~ XXX minutes

License:
    BSD, see LICENSE.md
"""
from typing import Optional, Union, Sequence
from pathlib import Path
from pprint import pprint
import json
import numpy as np
import pandas as pd
import xarray as xr
from spectral_trend_database.config import config as c
from spectral_trend_database import query
from spectral_trend_database import smoothing
from spectral_trend_database import utils
from spectral_trend_database import paths
from spectral_trend_database import gcp
import mproc


#
# CONSTANTS
#
YEARS = range(2000, 2022 + 1)
LIMIT = None
DRY_RUN = False

# YEARS = range(2003, 2003 + 1)
# LIMIT = 100
# DRY_RUN = False

TABLE_NAME = c.SMOOTHED_INDICES_TABLE_NAME.upper()
MAP_METHOD = mproc.map_sequential
# MAP_METHOD = mproc.map_with_threadpool


#
# METHODS
#
def get_data_vars(data: dict) -> list[str]:
    """ get data_var names from dataframe """
    return [
        column for column in data.keys()
        if column not in
        c.META_COLUMNS + [c.COORD_COLUMN]]


def append_ldjson(file_path: str, data: dict):
    with open(file_path, "a") as file:
        file.write(json.dumps(data) + "\n")


def post_process_row(row: dict, data_vars: list[str]) -> dict:
    row[c.DATE_COLUMN] = utils.cast_duck_array(row[c.DATE_COLUMN])
    for var in data_vars + [c.COORD_COLUMN]:
        row[var] = list(row[var])
    return row


def write_smooth_row(
        row: Union[pd.Series, dict],
        data_vars: list[str],
        file_path: str) -> Union[str, None]:
    """
    1. transform pandas row to xr.dataset
    2. mask data by MASK_EQ
    3. smooth with savitzky_golay_processor
    """
    try:
        ds = utils.row_to_xr(
            row,
            coord=c.COORD_COLUMN,
            data_vars=data_vars)
        if c.MASK_EQ:
            mask = ds.eval(c.MASK_EQ)
            ds = xr.where(mask, ds, np.nan).assign_attrs(ds.attrs)
        ds = smoothing.savitzky_golay_processor(ds, **c.SG_CONFIG)
        row = utils.xr_to_row(ds)
        row = post_process_row(row, data_vars)
        append_ldjson(file_path=file_path, data=row)
    except Exception as e:
        return str(e)
        raise e


def get_paths(year: int):
    file_name = f'{TABLE_NAME.lower()}-{year}.json'
    local_dest = paths.local(
        c.DEST_LOCAL_FOLDER,
        c.SMOOTHED_INDICES_FOLDER,
        file_name)
    gcs_dest = paths.gcs(
        c.DEST_GCS_FOLDER,
        c.SMOOTHED_INDICES_FOLDER,
        file_name)
    return local_dest, gcs_dest


#
# RUN
#
print('\nsmooth indices:')
print('-' * 50)
for year in YEARS:
    local_dest, gcs_dest = get_paths(year)
    data = query.run(
        table=c.RAW_INDEX_TABLE_NAME,
        year=year,
        limit=LIMIT)
    print()
    print(f'- year: {year}')
    data = data.to_dict('records')
    data_vars = get_data_vars(data[0])
    print(f'- nb_rows: {len(data)}')
    Path(local_dest).parent.mkdir(parents=True, exist_ok=True)
    print('- local_dest:', local_dest)
    errors = MAP_METHOD(
        lambda row: write_smooth_row(row, data_vars=data_vars, file_path=local_dest),
        data,
        max_processes=c.MAX_PROCESSES)
    if DRY_RUN:
        print('- gcs_dest:', gcs_dest, '- dry_run')
    else:
        uri = gcp.upload_file(local_dest, gcs_dest)
        print('- gcs_dest:', uri)
    print(f'- update table [{c.DATASET_NAME}.{TABLE_NAME}]')
    if DRY_RUN:
        print('- dry_run: table not updated')
    else:
        assert isinstance(uri, str)
        gcp.create_or_update_table_from_json(
            gcp.load_or_create_dataset(c.DATASET_NAME, c.LOCATION),
            name=TABLE_NAME,
            uri=uri)
    errors = [e for e in errors if e]
    if errors:
        print(f'- nb_errors: {len(errors)}')
        for e in list(set(errors)):
            print(' ', e)
