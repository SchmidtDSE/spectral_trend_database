""" COVER CROP FEATURES

authors:
    - name: Brookie Guzder-Williams

affiliations:
    - University of California Berkeley,
      The Eric and Wendy Schmidt Center for Data Science & Environment

steps:

    for smoothed daily indices ...
    compute:
        macd(div) features

outputs:


runtime: ~ XXX minutes

License:
    BSD, see LICENSE.md
"""
from typing import Callable, Union, Optional, Literal, TypeAlias, Sequence, Any
from datetime import timedelta
from dateutil.parser import parse as dt_parse
import pandas as pd
import mproc
from spectral_trend_database.config import config as c
from spectral_trend_database import query
from spectral_trend_database import smoothing
from spectral_trend_database import utils
from spectral_trend_database import paths
from spectral_trend_database import types
from spectral_trend_database import interface


#
# CONSTANTS
#
YEARS = range(c.YEARS[0], c.YEARS[1] + 1)
IDENT_COLS = ['sample_id', 'year', 'date']
# MAP_METHOD = mproc.map_sequential
MAP_METHOD = mproc.map_with_threadpool
SRC_INDICES = ['ndvi', 'evi', 'evi2']
COLUMNS = ['date', 'sample_id'] + SRC_INDICES
GROWING_YEAR_BUFFER = timedelta(days=20)


#
# METHODS
#
def process_rows(
        rows: pd.DataFrame,
        sample_id: str,
        year: int,
        start_date: str,
        end_date: str,
        local_dest: str,
        data_vars: list):
    try:
        ds = rows[['date'] + data_vars].set_index('date').to_xarray()
        ds = smoothing.macd_processor(ds, spans=[5, 10, 5])
        ds = ds.sel(date=slice(start_date, end_date))
        data = ds.to_dataframe().reset_index(drop=False)
        data['date'] = data.date.apply(lambda d: d.strftime(c.YYYY_MM_DD_FMT))
        data = data.to_dict('records')
        utils.append_ldjson(
            local_dest,
            data,
            sample_id=sample_id,
            year=year,
            multiline=True,
            dry_run=c.DRY_RUN)
    except Exception as e:
        return dict(sample_id=sample_id, year=year, error=str(e))


#
# RUN
#
print('\n' * 2)
print('compute macd(-div) series:')
print('=' * 100)


for year in YEARS:
    print(f'\n- year: {year}')
    # 1. process paths/dates
    table_name, local_dest, gcs_dest = interface.table_name_and_paths(
        c.MACD_FOLDER,
        table_name=c.MACD_TABLE_NAME,
        year=year)
    start = dt_parse(f'{year-1}-{c.OFF_SEASON_START_YYMM}')
    end = dt_parse(f'{year}-{c.OFF_SEASON_START_YYMM}')
    utils.make_parent_directories(local_dest)

    # 2. query data
    print('- run query:')
    qc = query.QueryConstructor(
        c.SMOOTHED_INDICES_TABLE_NAME,
        table_prefix=f'{c.GCP_PROJECT}.{c.DATASET_NAME}')
    qc.select(*COLUMNS)
    qc.where(
        date=(start - GROWING_YEAR_BUFFER).strftime(c.YYYY_MM_DD_FMT),
        date_op='>=')
    qc.where(
        date=(end + GROWING_YEAR_BUFFER).strftime(c.YYYY_MM_DD_FMT),
        date_op='<')
    data = query.run(
        sql=qc.sql(),
        to_dataframe=True,
        print_sql=True)
    data = data.sort_values('date')
    sample_ids = data.sample_id.unique()

    # 3. run
    data_vars = [n for n in data.columns if n not in IDENT_COLS]
    errors = MAP_METHOD(
        lambda s: process_rows(
            data[data.sample_id == s],
            sample_id=s,
            year=year,
            start_date=start.strftime(c.YYYY_MM_DD_FMT),
            end_date=end.strftime(c.YYYY_MM_DD_FMT),
            local_dest=local_dest,
            data_vars=data_vars),
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
