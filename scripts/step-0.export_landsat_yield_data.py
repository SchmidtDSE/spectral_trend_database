""" EXPORT LANDSAT/YIELD DATA

authors:
    - name: Brookie Guzder-Williams

affiliations:
    - University of California Berkeley,
      The Eric and Wendy Schmidt Center for Data Science & Environment

steps:

    1. Load Sample Yield data
    2. Add geohashes
    3. For each year:
        - Add Harmonized Landsat Pixel Values
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
YEARS = range(2008, 2022 + 1)
LIMIT = None
SCALE = 30
CROP_TYPE = 'corn'
# CROP_TYPE = 'soy'
BUFFER = 0
# SRC_PATH = f'{c.URL_PREFIX}agriculture_monitoring/CDL/landsat/{CROP_TYPE}_biomass_1999-2022.csv'
SRC_PATH = f'{c.URL_PREFIX}agriculture_monitoring/CDL/lobell/QDANN/{CROP_TYPE}_biomass_2008-2022.csv'
# BUFFER = 30
HARVEST_START_DATE_TMPL = '{}-09-01'
HARVEST_END_DATE_TMPL = '{}-12-01'
PRECISIONS = [5, 7, 9]
MAX_PROCESSES = 6
MAX_ERR = 1


#
# CONSTANTS
#
DATA_ROOT = f'{c.ROOT_DIR}/{c.LOCAL_DATA_DIR}'

DEST_NAME = f'{CROP_TYPE}_{c.RAW_BASE_NAME}'
LANDSAT_RADIUS = math.ceil(landsat.NOMINAL_SCALE / 2)
if BUFFER != 'auto':
    LANDSAT_RADIUS = LANDSAT_RADIUS + BUFFER
    DEST_NAME = f'{DEST_NAME}.b{BUFFER}'
if LIMIT:
    DEST_NAME = f'{DEST_NAME}-lim{LIMIT}'


#
# METHODS
#
def get_geohash(row: pd.Series, precision: int) -> str:
    return geohash.encode(
        latitude=row.lat,
        longitude=row.lon,
        precision=precision)


#
# METHODS
#
def get_mean_pixel_values(row: pd.Series) -> xr.Dataset:
    radius = LANDSAT_RADIUS
    if BUFFER == 'auto':
        buffer = row['buffer']
        if np.isnan(buffer):
            buffer = 0
        radius = LANDSAT_RADIUS + buffer
    start_date = HARVEST_START_DATE_TMPL.format(row['year'] - 1)
    end_date = HARVEST_END_DATE_TMPL.format(row['year'])
    geom = ee.Geometry.Point([row['lon'], row['lat']])
    geom = geom.buffer(radius, MAX_ERR)
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


def add_harvest_year_bands_ds(row):
    row = dict(row).copy()
    try:
        ds = get_mean_pixel_values(row)
        row.update({b: ds[b].data for b in landsat.HARMONIZED_BANDS})
        row['date'] = list(ds.time.dt.strftime('%Y-%m-%d').data)
        row['error'] = None
    except Exception as e:
        row['error'] = str(e)
    return row


#
# RUN
#
print('load data:')
# 1. Load Sample Yield data
print('-', SRC_PATH)
df = pd.read_csv(SRC_PATH)
print('- shape:', df.shape)
df = df.drop_duplicates()
print('- drop duplicates shape:', df.shape)

# 2. Add geohashes
df['sample_id'] = df.apply(lambda r: get_geohash(r, 11), axis=1)
for p in PRECISIONS:
    df[f'geohash_{p}'] = df.sample_id.apply(lambda v: v[:p])


# 3. For each year:
#     - Add Harmonized Landsat Pixel Values
#     - Save results, local and GCS, as line-deliminated JSON files
print(f'RUNNING LANDSAT EXPORT FOR {YEARS}')
for year in YEARS:
    print(year, '...')
    data = df[df.year == year].to_dict('records')[:LIMIT]
    count = len(data)
    if count:
        print(f'RUNNING {DEST_NAME}[{count}] FOR YEAR = {year}')
        rows = mproc.map_with_threadpool(
            add_harvest_year_bands_ds,
            data,
            max_processes=MAX_PROCESSES)
        lsat_df = pd.DataFrame(rows)
        name = f'{DEST_NAME}-{year}.json'
        local_dest = paths.local(
            c.RAW_LOCAL_FOLDER,
            name)
        gcs_dest = paths.gcs(
            c.RAW_GCS_FOLDER,
            name)
        uri = gcp.save_ld_json(
            lsat_df,
            local_dest=local_dest,
            gcs_dest=gcs_dest,
            dry_run=DRY_RUN)
        print(f'COMPLETE[{lsat_df.shape}]:', DEST_NAME)

    else:
        print(f'NO DATA FOR {year}')
