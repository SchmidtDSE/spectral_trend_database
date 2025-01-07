""" INDEX STATS

authors:
    - name: Brookie Guzder-Williams

affiliations:
    - University of California Berkeley,
      The Eric and Wendy Schmidt Center for Data Science & Environment

steps:

    Stats For Smoothed Indices

outputs:


runtime: ~ XXX minutes

License:
    BSD, see LICENSE.md
"""
from typing import Callable, Union, Optional, Literal, TypeAlias, Sequence, Any
import re
import pandas as pd
import xarray as xr
import mproc
from spectral_trend_database.config import config as c
from spectral_trend_database import gcp
from spectral_trend_database import query
from spectral_trend_database import paths
from spectral_trend_database import utils
from spectral_trend_database import types
from spectral_trend_database import runner


#
# CONSTANTS
#
YEARS = range(2006, 2011 + 1)
DRY_RUN = False
IDENT_COLS = ['sample_id', 'year', 'date']
MAP_METHOD = mproc.map_with_threadpool


#
# METHODS
#
def process_annual_data(
        rows: pd.DataFrame,
        sample_id: str,
        year: int,
        local_dest: str,
        local_dest_off: str,
        local_dest_grow: str,
        data_vars: list):
    try:
        ds = rows[['date'] + data_vars].set_index('date').to_xarray()
        ds_off = ds.sel(dict(date=slice(
            f'{year-1}-{c.OFF_SEASON_START_YYMM}',
            f'{year}-{c.OFF_SEASON_END_YYMM}')))
        ds_grow = ds.sel(dict(date=slice(
            f'{year-1}-{c.GROWING_SEASON_START_YYMM}',
            f'{year}-{c.GROWING_SEASON_END_YYMM}')))
        stats_ds = utils.xr_stats(ds, data_vars=data_vars)
        stats_off_ds = utils.xr_stats(ds_off, data_vars=data_vars)
        stats_grow_ds = utils.xr_stats(ds_grow, data_vars=data_vars)
        append_row(sample_id, year, stats_ds, local_dest)
        append_row(sample_id, year, stats_off_ds, local_dest_off)
        append_row(sample_id, year, stats_grow_ds, local_dest_grow)
    except Exception as e:
        return dict(sample_id=sample_id, year=year, error=str(e))


def append_row(sample_id: str, year: int, ds: xr.Dataset, dest: str):
    if DRY_RUN:
        print('- dry_run [local]:', dest)
    else:
        data = dict(sample_id=sample_id, year=year)
        data.update({
            k: float(ds.data_vars[k].values)
            for k in ds.data_vars})
        utils.append_ldjson(file_path=dest, data=data)


def period_ident(start_mmdd, end_mmdd):
    return re.sub('-', '', '_'.join([start_mmdd, end_mmdd]))


def append_name(path, *args: str, ext='json', sep='_', remove='-'):
    if remove:
        args = [re.sub(remove,'', a) for a in args]
    if ext:
        return re.sub(f'{ext}$', f'{sep.join(args)}.{ext}', path)
    else:
        args = [path] + list(args)
        return sep.join(args)



#
# RUN
#
print('\n' * 2)
print('=' * 100)
for year in YEARS:
    print('-' * 100)
    # 1. process paths
    growing_year_ident = period_ident(c.OFF_SEASON_START_YYMM, c.OFF_SEASON_START_YYMM)
    off_ident = period_ident(c.OFF_SEASON_START_YYMM, c.OFF_SEASON_END_YYMM)
    grow_ident = period_ident(c.GROWING_SEASON_START_YYMM, c.GROWING_SEASON_END_YYMM)

    table_name, local_dest, gcs_dest = runner.table_name_and_paths(
        c.INDICES_STATS_FOLDER,
        growing_year_ident,
        table_name=c.INDICES_STATS_TABLE_NAME,
        year=year)

    local_dest_off = re.sub(growing_year_ident, off_ident, local_dest)
    gcs_dest_off = re.sub(growing_year_ident, off_ident, gcs_dest)
    table_name_off = f'{table_name}_OFF_SEASON'
    local_dest_grow = re.sub(growing_year_ident, grow_ident, local_dest)
    gcs_dest_grow = re.sub(growing_year_ident, grow_ident, gcs_dest)
    table_name_grow = f'{table_name}_GROWING_SEASON'

    runner.make_directories(local_dest, local_dest_off, local_dest_grow)


    # 2. query data
    print('- run query:')
    qc = query.QueryConstructor(
        c.SMOOTHED_INDICES_TABLE_NAME,
        table_prefix=f'{c.GCP_PROJECT}.{c.DATASET_NAME}')
    qc.where(date=f'{year-1}-{c.OFF_SEASON_START_YYMM}', date_op='>=')
    qc.where(date=f'{year}-{c.OFF_SEASON_START_YYMM}', date_op='<')
    data = query.run(
        sql=qc.sql(),
        to_dataframe=True,
        print_sql=True)
    data = data.sort_values('date')
    sample_ids = data.sample_id.unique()


    # 3. run
    data_vars = [n for n in data.columns if n not in IDENT_COLS]
    errors = MAP_METHOD(
        lambda s: process_annual_data(
            data[data.sample_id==s],
            sample_id=s,
            year=year,
            local_dest=local_dest,
            local_dest_off=local_dest_off,
            local_dest_grow=local_dest_grow,
            data_vars=data_vars),
        sample_ids,
        max_processes=c.MAX_PROCESSES)


    # 4. gcp
    if DRY_RUN:
        print('- dry_run [gcp]:')
        print('\t',gcs_dest)
        print('\t',gcs_dest_off)
        print('\t',gcs_dest_grow)
    else:
        runner.save_to_gcp(
            src=local_dest,
            gcs_dest=gcs_dest,
            dataset_name=c.DATASET_NAME,
            table_name=table_name,
            remove_src=True)
        runner.save_to_gcp(
            src=local_dest_off,
            gcs_dest=gcs_dest_off,
            dataset_name=c.DATASET_NAME,
            table_name=table_name_off,
            remove_src=True)
        runner.save_to_gcp(
            src=local_dest_grow,
            gcs_dest=gcs_dest_grow,
            dataset_name=c.DATASET_NAME,
            table_name=table_name_grow,
            remove_src=True)

    # 5. report on errors
    runner.print_errors(errors)
    print('\n' * 2)
