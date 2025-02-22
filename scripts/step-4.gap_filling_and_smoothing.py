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
from spectral_trend_database import utils
from spectral_trend_database import interface
from spectral_trend_database.gee import landsat
import mproc


#
# CONSTANTS
#
YEARS = range(c.YEARS[0], c.YEARS[1] + 1)
MAP_METHOD = mproc.map_with_threadpool
# MAP_METHOD = mproc.map_sequential
YEAR_BUFFER = relativedelta(days=smoothing.DEFAULT_SG_WINDOW_LENGTH * 2)
YEAR_DELTA = relativedelta(years=1)
DS_COLUMNS = ['date'] + landsat.HARMONIZED_BANDS + list(spectral.index_config().keys())
ORDERED_COLUMNS = ['sample_id', 'year'] + DS_COLUMNS


#
# METHODS
#
def append_ldjson(file_path: str, data: dict):
    with open(file_path, "a") as file:
        file.write(json.dumps(data) + "\n")


def post_process_row(row: dict, data_vars: list[str]) -> dict:
    row[c.DATE_COLUMN] = utils.cast_duck_array(row[c.DATE_COLUMN])
    for var in data_vars + ['date']:
        row[var] = list(row[var])
    return row


def process_smoothing(
        rows: Union[pd.Series, dict],
        year: int,
        sample_id: str,
        dest: str) -> Union[str, None]:
    try:
        rows = rows[DS_COLUMNS].copy()
        rows = rows[rows.ndvi > 0]
        ds = rows.set_index('date').to_xarray()
        ds = smoothing.savitzky_golay_processor(ds, **c.SG_CONFIG)
        ds = ds.sel(dict(date=slice(c.JAN1_TMPL.format(year), c.DEC31_TMPL.format(year))))
        rows = ds.to_dataframe().reset_index(drop=False)
        rows['sample_id'] = sample_id
        rows['year'] = year
        rows = rows[ORDERED_COLUMNS]
        utils.dataframe_to_ldjson(
            rows,
            dest=dest,
            mode='a',
            noisy=False)
    except Exception as e:
        return dict(
            sample_id=sample_id,
            year=year,
            error=str(e))


#
# RUN
#
print('\nsmooth indices:')
print('-' * 50)
for year in YEARS:
    print(f'\n- year: {year}')
    # 1. process paths
    table_name, local_dest, gcs_dest = interface.table_name_and_paths(
        c.SMOOTHED_INDICES_FOLDER,
        table_name=c.SMOOTHED_INDICES_TABLE_NAME,
        year=year)

    # 2. query data
    jan1 = datetime(year=year, month=1, day=1)
    start = (jan1 - YEAR_BUFFER).strftime(c.YYYY_MM_DD_FMT)
    end = (jan1 + YEAR_DELTA + YEAR_BUFFER).strftime(c.YYYY_MM_DD_FMT)
    qc = query.QueryConstructor(
        c.RAW_INDICES_TABLE_NAME,
        table_prefix=f'{c.GCP_PROJECT}.{c.DATASET_NAME}')
    qc.where(date=start, date_op='>=')
    qc.where(date=end, date_op='<=')
    qc.append('ORDER BY date ASC')
    data = query.run(sql=qc.sql())
    sample_ids = data.sample_id.unique()

    # 3. run
    errors = MAP_METHOD(
        lambda s: process_smoothing(
            data[data.sample_id == s],
            sample_id=s,
            year=year,
            dest=local_dest),
        sample_ids,
        max_processes=c.MAX_PROCESSES)

    # 4. report on errors
    interface.print_errors(errors)

    # 5. save data (gcs, bq)
    interface.save_to_gcp(
        src=local_dest,
        gcs_dest=gcs_dest,
        dataset_name=c.DATASET_NAME,
        table_name=table_name,
        remove_src=True,
        dry_run=c.DRY_RUN)
