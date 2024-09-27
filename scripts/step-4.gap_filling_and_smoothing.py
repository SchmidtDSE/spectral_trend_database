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


#
# CONSTANTS
#
YEARS = range(2000, 2022 + 1)
LIMIT = None
DRY_RUN = False

#
# METHODS
#
def get_data_vars(df: pd.DataFrame) -> list[str]:
    """ get data_var names from dataframe """
    return [
        column for column in df.columns
        if column not in
        c.META_COLUMNS + [c.COORD_COLUMN]]


def smooth_row(row: pd.Series, data_vars: list[str]) -> Union[dict, pd.Series]:
    """
    1. transform pandas row to xr.dataset
    2. mask data by MASK_EQ
    3. smooth with savitzky_golay_processor
    """
    try:
        ds = utils.pandas_to_xr(
            df.sample().iloc[0],
            coord=c.COORD_COLUMN,
            data_vars=data_vars)
        if c.MASK_EQ:
            mask = ds.eval(c.MASK_EQ)
            ds = xr.where(mask, ds, np.nan).assign_attrs(ds.attrs)
        ds = smoothing.savitzky_golay_processor(ds, **c.SG_CONFIG)
        row = utils.xr_to_row(ds)
        error = None
    except Exception as e:
        row = {}
        error = str(e)
    row['error'] = error
    return row


def smooth_indices(df: pd.DataFrame) -> pd.DataFrame:
    """
    1. query database for year
    2. for each row
        a. get dataset
        b.
    """
    data_vars = get_data_vars(df)
    rows = df.apply(lambda r: smooth_row(r, data_vars), axis=1)
    return pd.DataFrame(rows)


#
# RUN
#
print('\nsmooth indices:')
print('-' * 50)
for year in YEARS:
    df = query.run(
        table=c.RAW_INDEX_TABLE_NAME,
        year=year,
        limit=LIMIT)
    print()
    print(f'year: {year}')
    print(f'data-shape: {df.shape}')
    df = smooth_indices(df)
    print(f'output-shape: {df.shape}')
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
        df,
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
