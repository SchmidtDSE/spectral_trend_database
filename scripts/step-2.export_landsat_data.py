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
from spectral_trend_database import runner
from spectral_trend_database import utils
from spectral_trend_database.gee import landsat
from spectral_trend_database.gee import utils as ee_utils
warnings.filterwarnings(
    action="ignore",
    message="invalid value encountered in divide",
)


#
# CONFIG
#
YEARS = range(c.YEARS[0], c.YEARS[1] + 1)
MAX_PROCESSES = 6
MAX_ERR = 1


#
# CONSTANTS
#
SRC_PATH = gcs_dest = paths.gcs(
    c.RAW_GCS_FOLDER,
    c.SAMPLE_POINTS_TABLE_NAME,
    ext='json')

ORDERED_COLUMNS = ['sample_id', 'year', 'date', 'nb_images_in_year']
ORDERED_COLUMNS += landsat.HARMONIZED_BANDS
MAP_METHOD = mproc.map_with_threadpool
# MAP_METHOD = mproc.map_sequential


#
# METHODS
#
def get_mean_pixel_values(row: pd.Series, year=int) -> xr.Dataset:
    start_date = c.JAN1_TMPL.format(year)
    end_date = c.JAN1_TMPL.format(year + 1)
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


def process_mean_pixel_rows(
        row: dict,
        year: int,
        dest: str):
    row = dict(row).copy()
    sample_id = row['sample_id']
    ds = get_mean_pixel_values(row, year)
    try:
        ds = get_mean_pixel_values(row, year)
        rows = ds.to_dataframe().reset_index(names='date')
        rows = rows.dropna(subset=landsat.HARMONIZED_BANDS, how='all')
        rows = filter_missing_and_cloud_data(rows)
        rows['sample_id'] = sample_id
        rows['year'] = year
        rows['nb_images_in_year'] = len(rows)
        rows = rows[ORDERED_COLUMNS].sort_values(['date'])
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


def filter_missing_and_cloud_data(df):
    df = df[~df.green.isna() | (df.nir < df.red)].copy()
    green_exists = df.green.apply(_is_truthy)
    return df[green_exists].copy()


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
if c.LIMIT:
    df = df.sample(c.LIMIT)
    print('- limit shape:', df.shape)
data = df.to_dict('records')


# 2. For each year:
#     - Get Mean Harmonized Landsat Values
#     - Save results, local and GCS, as line-deliminated JSON files
print(f'RUNNING LANDSAT EXPORT FOR {YEARS}')
for year in YEARS:
    print(f'\n- year: {year}')
    # 1. process paths
    _, local_dest, gcs_dest = runner.table_name_and_paths(
        c.RAW_LANDSAT_FOLDER,
        file_name=c.RAW_LANDSAT_FILENAME,
        year=year)

    # 2. run
    errors = MAP_METHOD(
        lambda r: process_mean_pixel_rows(r, year=year, dest=local_dest),
        data,
        max_processes=MAX_PROCESSES)
    runner.print_errors(errors)

    # 3. report on errors
    runner.print_errors(errors)

    # 4. save data (gcs, bq)
    runner.save_to_gcp(
        src=local_dest,
        gcs_dest=gcs_dest,
        dataset_name=c.DATASET_NAME,
        table_name=None,
        remove_src=True,
        dry_run=c.DRY_RUN)
