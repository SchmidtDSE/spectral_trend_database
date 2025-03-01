""" PROCESS YIELD DATA

authors:
    - name: Brookie Guzder-Williams

affiliations:
    - University of California Berkeley,
      The Eric and Wendy Schmidt Center for Data Science & Environment

steps:

    1. Load Sample Yield data
    2. Add sample_id (geohash-11)
    3. add h3
    4. Requrie unique lon-lat per h3-(c.UNIQUE_H3)
    5. require `c.MIN_REQUIRED_YEARS` per h3-5
    6. sort data
    7. extract samples and merge political data
    8. save samples data
    9. collect and rename yield data
    10. save yield data

outputs:

    sample data [(15207, 8)]:
        - local: /Users/brookieguzder-williams/code/dse/COVERCROPS/spectral_trend_database/data/v1/DEV_ASROWS/qdann/raw/biomass_landsat/sample_points.json
        - gcs: gs://agriculture_monitoring/spectral_trend_database/v1/DEV_ASROWS/qdann/raw/biomass_landsat/sample_points.json

    yield data [(332273, 4)]:
        - local: /Users/brookieguzder-williams/code/dse/COVERCROPS/spectral_trend_database/data/v1/DEV_ASROWS/qdann/raw/biomass_landsat/qdann_yield.json
        - gcs: gs://agriculture_monitoring/spectral_trend_database/v1/DEV_ASROWS/qdann/raw/biomass_landsat/qdann_yield.json

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
import geopandas as gpd
import xarray as xr
import h3
import mproc  # type: ignore[import-untyped]
from spectral_trend_database.config import config as c
from spectral_trend_database import gcp
from spectral_trend_database import paths
from spectral_trend_database import utils
from spectral_trend_database import interface
from spectral_trend_database.gee import landsat
from spectral_trend_database.gee import utils as ee_utils
warnings.filterwarnings(
    action="ignore",
    message="invalid value encountered in divide",
)


#
# CONSTANTS
#
CROP_TYPES = ['corn', 'soy']
LL = ['lon', 'lat']
SAMPLE_COLS = ['sample_id', 'lon', 'lat'] + [f'h3_{r}' for r in c.H3_RESOLUTIONS]
YIELD_DATA_COLS = [
    'sample_id',
    'year',
    'biomass',
    'nb_years',
    'crop_type',
    'crop_label']
YIELD_RENAME = dict(crop_type='qdann_crop_type', crop_label='qdann_crop_label')


#
# METHODS
#
def path_for_crop_type(crop_type):
    path = f'{c.URL_PREFIX}{c.GCS_BUCKET}/{c.GCS_RAW_SAMPLES_DIR}'
    if c.YIELD_BUFFER_RADIUS:
        path += f'/{crop_type}_biomass_2008-2022-b{c.YIELD_BUFFER_RADIUS}-mean.csv'
    else:
        path += f'/{crop_type}_biomass_2008-2022.csv'
    print(f'- {crop_type}-path:', path)
    return path


def load_data(*crop_types):
    return pd.concat([pd.read_csv(path_for_crop_type(n)) for n in crop_types])


def get_geohash(row: pd.Series, precision: int) -> str:
    return geohash.encode(
        latitude=row.lat,
        longitude=row.lon,
        precision=precision)


def get_h3(row: pd.Series, resolution: int) -> str:
    return h3.latlng_to_cell(row.lat, row.lon, resolution)


def unique_by_h3(df):
    df = df.copy()
    lonlats = df.groupby(f'h3_{c.UNIQUE_H3}').first()[LL]
    return df.merge(lonlats, how='inner', on=LL)


def require_min_years_per_h3(df, min_years=c.MIN_REQUIRED_YEARS):
    h3_key = f'h3_{c.UNIQUE_H3}'
    _df = df.copy().drop_duplicates(subset=['year', h3_key])
    h3_count = _df.groupby([h3_key]).size().reset_index(name='nb_years')
    df = df.merge(h3_count, on=h3_key, how='inner')
    if min_years:
        df = df[df.nb_years >= min_years]
    return df


def merge_county_data(df, us_gdf, rsuffix='political', drop_cols=['index_political', 'geometry']):
    gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.lon, df.lat), crs="EPSG:4326")
    gdf = gdf.sjoin(us_gdf, how='inner', rsuffix=rsuffix)
    if drop_cols:
        gdf = gdf.drop(drop_cols, axis=1)
    return gdf


#
# RUN
#
print('load data:')
# 1. Load Sample Yield data
df = load_data(*CROP_TYPES)
print('- data shape:', df.shape)
is_unique = (df.shape[0] == df.drop_duplicates(['lon', 'lat', 'year']).shape[0])
print(f'- unique by year and location: {is_unique}')


# 2. Add sample_id (geohash-11)
df['sample_id'] = df.apply(lambda r: get_geohash(r, 11), axis=1)
print(f'- sample_id shape:', df.shape)


# 3. add h3
for res in c.H3_RESOLUTIONS:
    df[f'h3_{res}'] = df.apply(lambda r: get_h3(r, res), axis=1)
print(f'- h3 shape:', df.shape)


# 4. Requrie unique lon-lat per h3-(c.UNIQUE_H3)
df = unique_by_h3(df)
print(f'- unique h3_{c.UNIQUE_H3} shape:', df.shape)


# 5. require `c.MIN_REQUIRED_YEARS` per h3-5
df = require_min_years_per_h3(df, min_years=c.MIN_REQUIRED_YEARS)
print(f'- min-years shape:', df.shape)


# 6. sort data
df = df.sort_values(['year', 'sample_id'])


# 7. extract samples and merge political data
_political_path = paths.local(c.US_BOUNDARIES_SHP)
utils.download_and_extract_zip(
    url=c.US_BOUNDARIES_URL,
    root_folder=Path(_political_path).parent)
_us_gdf = gpd.read_file(_political_path)
_us_gdf = _us_gdf.to_crs(epsg=4326)
samples_df = df.drop_duplicates(subset=['sample_id'])
samples_df = samples_df[SAMPLE_COLS].sort_values('sample_id')
samples_df = merge_county_data(samples_df, _us_gdf)
print(f'- samples shape:', samples_df.shape)


# 8. save samples data
table_name, local_dest, gcs_dest = interface.table_name_and_paths(
    c.SAMPLES_FOLDER,
    table_name=c.SAMPLE_POINTS_TABLE_NAME)
local_dest = utils.dataframe_to_ldjson(
    samples_df,
    dest=local_dest,
    dry_run=c.DRY_RUN)
interface.save_to_gcp(
    src=local_dest,
    gcs_dest=gcs_dest,
    dataset_name=c.DATASET_NAME,
    table_name=table_name,
    remove_src=True,
    dry_run=c.DRY_RUN)


# 9. collect and rename yield data
yield_df = df[YIELD_DATA_COLS].sort_values(['year', 'sample_id'])
yield_df = yield_df.rename(columns=YIELD_RENAME)
print(f'- yield shape:', yield_df.shape)


# 10. save yield data
table_name, local_dest, gcs_dest = interface.table_name_and_paths(
    c.QDANN_YIELD_FOLDER,
    table_name=c.QDANN_YIELD_TABLE_NAME)
local_dest = utils.dataframe_to_ldjson(
    yield_df,
    dest=local_dest,
    dry_run=c.DRY_RUN)
interface.save_to_gcp(
    src=local_dest,
    gcs_dest=gcs_dest,
    dataset_name=c.DATASET_NAME,
    table_name=table_name,
    remove_src=True,
    dry_run=c.DRY_RUN)
