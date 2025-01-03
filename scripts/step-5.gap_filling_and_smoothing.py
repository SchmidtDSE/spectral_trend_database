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
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pathlib import Path
from pprint import pprint
import json
import numpy as np
import pandas as pd
import xarray as xr
from spectral_trend_database.config import config as c
from spectral_trend_database import query
from spectral_trend_database import smoothing
from spectral_trend_database import spectral
from spectral_trend_database import utils
from spectral_trend_database import paths
from spectral_trend_database import gcp
from spectral_trend_database import types
from spectral_trend_database.gee import landsat
import mproc


#
# CONSTANTS
#
_indices = spectral.index_config()

YEARS = range(2009, 2011 + 1)
LIMIT = None
DRY_RUN = True

# YEARS = range(2003, 2003 + 1)
# LIMIT = 100
# DRY_RUN = False

TABLE_NAME = c.SMOOTHED_INDICES_TABLE_NAME.upper()
MAP_METHOD = mproc.map_sequential
# MAP_METHOD = mproc.map_with_threadpool

YEAR_BUFFER = relativedelta(days=smoothing.DEFAULT_SG_WINDOW_LENGTH * 2)
YEAR_DELTA = relativedelta(years=1)
DS_COLUMNS = ['date'] + landsat.HARMONIZED_BANDS + list(_indices.keys())


#
# METHODS
#
def append_ldjson(file_path: str, data: dict):
    with open(file_path, "a") as file:
        file.write(json.dumps(data) + "\n")


def post_process_row(row: dict, data_vars: list[str]) -> dict:
    row[c.DATE_COLUMN] = utils.cast_duck_array(row[c.DATE_COLUMN])
    for var in data_vars + [c.COORD_COLUMN]:
        row[var] = list(row[var])
    return row


def apply_smoothing(
        rows: Union[pd.Series, dict],
        year: int,
        sample_id: str,
        file_path: str) -> Union[str, None]:
    _df = rows[DS_COLUMNS].copy()
    _df = _df[_df.ndvi>0]
    ds = _df.set_index('date').to_xarray()
    ds = smoothing.savitzky_golay_processor(ds, **c.SG_CONFIG)
    ds = ds.sel(dict(date=slice(f'{year}-01-01', f'{year}-12-31')))
    print('BUG/FIX/WARNING: PTS HAVE MISSING YEARS - MISSING DATES SOMETIMES')
    _df = ds.to_dataframe().reset_index(drop=False)
    _df['sample_id'] = sample_id
    _df['year'] = year
    return _df


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


from IPython.display import display



for year in YEARS:
    local_dest, gcs_dest = get_paths(year)
    jan1 = datetime(year=year, month=1, day=1)
    start = (jan1 - YEAR_BUFFER).strftime('%Y-%m-%d')
    end = (jan1 + YEAR_DELTA  + YEAR_BUFFER).strftime('%Y-%m-%d')
    qc = query.QueryConstructor(
            c.RAW_LANDSAT_TABLE_NAME,
            table_prefix=f'{c.GCP_PROJECT}.{c.DATASET_NAME}',
            using=['sample_id', 'date'])
    qc.join(c.RAW_INDICES_TABLE_NAME)
    qc.where(date=start, date_op='>=')
    qc.where(date=end, date_op='<=')
    print(qc.sql())
    data = query.run(
        sql=qc.sql(),
        limit=LIMIT,
        append='ORDER BY date')
    sample_ids = data.sample_id.unique()
    print()
    print(f'- year: {year}')
    Path(local_dest).parent.mkdir(parents=True, exist_ok=True)
    print('- local_dest:', local_dest)
    dfs = MAP_METHOD(
        lambda s: apply_smoothing(
            data[data.sample_id==s],
            sample_id=s,
            year=year,
            file_path=local_dest),
        sample_ids,
        max_processes=c.MAX_PROCESSES)
    df = pd.concat(dfs)
    df = df[['sample_id', 'year'] + DS_COLUMNS]
    if DRY_RUN:
        print('- dry_run:')
        print('\t- local_dest:', local_dest)
        print('\t- gcs_dest:', gcs_dest)
    else:
        uri = gcp.save_ld_json(local_dest,gcs_dest)
        print('- local_dest:', local_dest)
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