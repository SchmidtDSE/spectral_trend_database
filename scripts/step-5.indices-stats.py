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
import warnings
import re
import numpy as np
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
YEARS = range(c.YEARS[0], c.YEARS[1] + 1)
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
        with warnings.catch_warnings():
            warnings.filterwarnings('error')
            try:
                stats_ds = utils.xr_stats(ds, data_vars=data_vars)
                stats_off_ds = utils.xr_stats(ds_off, data_vars=data_vars)
                stats_grow_ds = utils.xr_stats(ds_grow, data_vars=data_vars)
                append_row(sample_id, year, stats_ds, local_dest)
                append_row(sample_id, year, stats_off_ds, local_dest_off)
                append_row(sample_id, year, stats_grow_ds, local_dest_grow)
            except Warning as w:
                return dict(sample_id=sample_id, year=year, warning=str(w), error=None)
    except Exception as e:
        return dict(sample_id=sample_id, year=year, warning=None, error=str(e))


def append_row(
        sample_id: str,
        year: int,
        ds: xr.Dataset,
        dest: str,
        skip_na: Union[bool, Sequence[str]] = True,
        raise_warning: bool = True) -> None:
    if c.DRY_RUN:
        print('- dry_run [local]:', dest)
    else:
        data = dict(sample_id=sample_id, year=year)
        append = True
        if skip_na:
            if not isinstance(skip_na, list):
                skip_na = [k for k in ds.data_vars if k not in IDENT_COLS]
            test = np.array([ds.data_vars[k].values for k in skip_na])
            if np.isnan(test).all():
                append = False
                if raise_warning:
                    raise RuntimeWarning('append_row: empty row encountered.')
        if append:
            data.update({
                k: float(ds.data_vars[k].values)
                for k in ds.data_vars})
            utils.append_ldjson(dest, data=data)


def period_ident(start_mmdd, end_mmdd):
    return re.sub('-', '', '_'.join([start_mmdd, end_mmdd]))


#
# RUN
#
for year in YEARS:
    print(f'\n- year: {year}')
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
    utils.make_parent_directories(local_dest, local_dest_off, local_dest_grow)

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
            data[data.sample_id == s],
            sample_id=s,
            year=year,
            local_dest=local_dest,
            local_dest_off=local_dest_off,
            local_dest_grow=local_dest_grow,
            data_vars=data_vars),
        sample_ids,
        max_processes=c.MAX_PROCESSES)

    # 4. report on errors
    runner.print_errors(errors)

    # 5. save data (gcs, bq]
    runner.save_to_gcp(
        src=local_dest,
        gcs_dest=gcs_dest,
        dataset_name=c.DATASET_NAME,
        table_name=table_name,
        remove_src=True,
        dry_run=c.DRY_RUN)
    runner.save_to_gcp(
        src=local_dest_off,
        gcs_dest=gcs_dest_off,
        dataset_name=c.DATASET_NAME,
        table_name=table_name_off,
        remove_src=True,
        dry_run=c.DRY_RUN)
    runner.save_to_gcp(
        src=local_dest_grow,
        gcs_dest=gcs_dest_grow,
        dataset_name=c.DATASET_NAME,
        table_name=table_name_grow,
        remove_src=True,
        dry_run=c.DRY_RUN)
