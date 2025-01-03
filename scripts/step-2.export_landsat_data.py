""" EXPORT LANDSAT/YIELD DATA

authors:
    - name: Brookie Guzder-Williams

affiliations:
    - University of California Berkeley,
      The Eric and Wendy Schmidt Center for Data Science & Environment

steps:

    1. Load Sample Yield data
    2. For each year:
        - Get Mean Harmonized Landsat Values
        - Save results, local and GCS, as line-deliminated JSON files

outputs:

- local:
- gcs:

runtime: ~ XXX minutes

License:
    BSD, see LICENSE.md
"""
import ee
ee.Initialize()
import re
from pathlib import Path
import warnings
import math
import geohash  # type: ignore[import-untyped]
import numpy as np
import pandas as pd
import xarray as xr
import mproc  # type: ignore[import-untyped]
from spectral_trend_database.config import config as c
from spectral_trend_database import gcp
from spectral_trend_database import paths
from spectral_trend_database.gee import landsat
from spectral_trend_database.gee import utils as ee_utils
warnings.filterwarnings(
    action="ignore",
    message="invalid value encountered in divide",
)


#
# CONFIG
#
DRY_RUN = False  # TODO: CONFIG OR CML ARG
YEARS = range(2000, 2022 + 1)
LIMIT = None
YEARS = range(2004, 2011 + 1)
LIMIT = 10
MAX_PROCESSES = 6
MAX_ERR = 1


#
# CONSTANTS
#
JAN1_TMPL = '{}-01-01'
SRC_PATH = gcs_dest = paths.gcs(
    c.RAW_GCS_FOLDER,
    c.SAMPLE_POINTS_TABLE_NAME,
    ext='json')

ORDERED_COLUMNS = ['sample_id', 'year', 'date']
ORDERED_COLUMNS += landsat.HARMONIZED_BANDS
ORDERED_COLUMNS += ['images_for_year', 'error']


#
# METHODS
#
def get_mean_pixel_values(row: pd.Series, year=int) -> xr.Dataset:
    start_date = JAN1_TMPL.format(year)
    end_date = JAN1_TMPL.format(year+1)
    geom = ee.Geometry.Point([row['lon'], row['lat']])
    geom = geom.buffer(c.LANDSAT_BUFFER_RADIUS, MAX_ERR)
    data_filter = ee.Filter.And(
        ee.Filter.bounds(geom),
        ee.Filter.date(start_date, end_date))
    ic = landsat.harmonized_cloud_masked_rescaled_ic(data_filter=data_filter)
    ds = ee_utils.get_ee_xrr(
        ic,
        geom=geom,
        scale=landsat.NOMINAL_SCALE)
    ds = ds.mean(dim=('X', 'Y'), skipna=True)
    return ds


def get_mean_pixel_rows(row, year):
    row = dict(row).copy()
    sample_id = row['sample_id']
    try:
        ds = get_mean_pixel_values(row, year)
        rows = ds.to_dataframe().reset_index(names='date')
        rows = rows.dropna(subset=landsat.HARMONIZED_BANDS, how='all')
        rows['sample_id'] = sample_id
        rows['year'] = year
        rows['images_for_year'] = len(rows)
        rows['error'] = None
    except Exception as e:
        rows = pd.DataFrame([
            dict(
                sample_id=sample_id,
                year=year,
                error=str(e))])
    return rows


def filter_missing_and_cloud_data(df):
    df = df[~df.green.isna() | (df.nir < df.red)].copy()
    green_exists = df.green.apply(_is_truthy)
    return df[green_exists].copy()


def process_date_column(df):
    df = df.rename(columns=dict(time='date'))
    df = df.sort_values(by='date')
    return df


def _is_truthy(value):
    if value:
        return True
    else:
        return False


#
# RUN
#
print('load data:')
# 1. Load Sample Yield data
print('-', SRC_PATH)
df = pd.read_json(SRC_PATH, lines=True)
print('- shape:', df.shape)
if LIMIT:
    df = df.sample(LIMIT)
    print('- limit shape:', df.shape)

data = df.to_dict('records')


# 2. For each year:
#     - Get Mean Harmonized Landsat Values
#     - Save results, local and GCS, as line-deliminated JSON files
print(f'RUNNING LANDSAT EXPORT FOR {YEARS}')
for year in YEARS:
    print('-', year, '...')
    dfs = mproc.map_with_threadpool(
        lambda r: get_mean_pixel_rows(r, year=year),
        data,
        max_processes=MAX_PROCESSES)
    df = pd.concat(dfs)
    df = filter_missing_and_cloud_data(df)
    df = process_date_column(df)
    df = df.reset_index(drop=True)
    df = df[ORDERED_COLUMNS].sort_values(['sample_id', 'date'])
    if df.shape[0]:
        name = f'{c.RAW_LANDSAT_TABLE_NAME}-{year}.json'
        local_dest = paths.local(
            c.RAW_LOCAL_FOLDER,
            name)
        gcs_dest = paths.gcs(
            c.RAW_GCS_FOLDER,
            name)
        uri = gcp.save_ld_json(
            df,
            local_dest=local_dest,
            gcs_dest=gcs_dest,
            dry_run=DRY_RUN)
        print(f'COMPLETE[{df.shape}]:', name)
    else:
        print(f'NO DATA FOR {year}')
