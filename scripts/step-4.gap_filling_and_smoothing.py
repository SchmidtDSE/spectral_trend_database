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
from pprint import pprint
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
# YEARS = range(2000, 2022 + 1)
# LIMIT = None
# DRY_RUN = False

YEARS = range(2011, 2012 + 1)
LIMIT = 100
DRY_RUN = False

#
# METHODS
#
def get_data_vars(data: dict) -> list[str]:
    """ get data_var names from dataframe """
    return [
        column for column in data.keys()
        if column not in
        c.META_COLUMNS + [c.COORD_COLUMN]]


def smooth_row(row: Union[pd.Series, dict], data_vars: list[str]) -> Union[dict, pd.Series]:
    """
    1. transform pandas row to xr.dataset
    2. mask data by MASK_EQ
    3. smooth with savitzky_golay_processor
    """
    try:
        ds = utils.row_to_xr(
            df.sample().iloc[0],
            coord=c.COORD_COLUMN,
            data_vars=data_vars)
        if c.MASK_EQ:
            mask = ds.eval(c.MASK_EQ)
            ds = xr.where(mask, ds, np.nan).assign_attrs(ds.attrs)
        ds = smoothing.savitzky_golay_processor(ds, **c.SG_CONFIG)
        row = utils.xr_to_row(ds)
        row[c.DATE_COLUMN] = utils.cast_duck_array(c.DATE_COLUMN)
        error = None
    except Exception as e:
        row = {}
        error = str(e)
    row['error'] = error
    return row


#
# RUN
#
print('\nsmooth indices:')
print('-' * 50)
for year in YEARS:
    data = query.run(
        table=c.RAW_INDEX_TABLE_NAME,
        year=year,
        limit=LIMIT)
    print()
    print(f'year: {year}')
    print(f'(df) data-shape: {data.shape[0]}')
    data = data.to_dict('records')
    data_vars = get_data_vars(data[0])
    print(f'(list[dict]) data-shape: {len(data)}')
    data = mproc.map_with_threadpool(
        lambda row: smooth_row(row, data_vars=data_vars),
        data,
        max_processes=c.MAX_PROCESSES)
    print(f'(list[dict]) output-shape: {len(data)}')
    table_name = c.SMOOTHED_INDICES_TABLE_NAME.upper()
    file_name = f'{table_name.lower()}-{year}.json'
    local_dest = paths.local(
        c.DEST_LOCAL_FOLDER,
        c.SMOOTHED_INDICES_FOLDER,
        file_name)
    gcs_dest = paths.gcs(
        c.DEST_GCS_FOLDER,
        c.SMOOTHED_INDICES_FOLDER,
        file_name)
    uri = gcp.save_ld_json(
        pd.DataFrame(data),
        local_dest=local_dest,
        gcs_dest=gcs_dest,
        dry_run=DRY_RUN)
    print(f'- update table [{c.DATASET_NAME}.{table_name}]')
    if DRY_RUN:
        print('- dry_run: table not updated')
    else:
        assert isinstance(uri, str)
        gcp.create_or_update_table_from_json(
            gcp.load_or_create_dataset(c.DATASET_NAME, c.LOCATION),
            name=table_name,
            uri=uri)
