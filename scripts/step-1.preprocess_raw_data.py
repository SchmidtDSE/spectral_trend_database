""" PRE-PROCESS BIOMASS YIELD DATA

authors:
    - name: Brookie Guzder-Williams

affiliations:
    - University of California Berkeley,
      The Eric and Wendy Schmidt Center for Data Science & Environment

steps:

    1. Load and Concatenate CSVs from GCP
    2. Remove missing band values (tested using green only)
    3. Requrie unique lon-lat per geohash-7
    4. remove nan/none values from coord-arrays
    5. require `c.MIN_REQUIRED_YEARS` per geohash
    6. Add County/State Data
    7. Save results, local and GCS, as line-deliminated JSON

outputs:

    local: c.ROOT_DIR/c.LOCAL_DATA_DIR/c.DEST_LOCAL_FOLDER/c.DEST_BIOMASS_YIELD_NAME
    gcs: gs://c.GCS_BUCKET/c.GCS_ROOT_FOLDER/c.DEST_GCS_FOLDER/c.DEST_BIOMASS_YIELD_NAME

runtime: ~ 20 minutes

License:
    BSD, see LICENSE.md
"""
from pathlib import Path
import numpy as np
import pandas as pd
import geopandas as gpd  # type: ignore[import-untyped]
from spectral_trend_database.config import config as c
from spectral_trend_database import gcp
from spectral_trend_database import paths
from spectral_trend_database import utils
from spectral_trend_database.gee import landsat


#
# CONSTANTS
#
DRY_RUN = False  # TODO: CONFIG OR CML ARG
DEV_NB_SAMPLES = False  # TODO: CONFIG OR CML ARG
SEARCH = c.SEARCH  # TODO: CONFIG OR CML ARG
COUNTY_ID = 'GEOID'
LL = ['lon', 'lat']
LOCAL_DIR = f'{c.LOCAL_DATA_DIR}/{c.DEST_LOCAL_FOLDER}'
LIST_COLUMNS = [c.DATE_COLUMN] + landsat.HARMONIZED_BANDS


#
# METHODS
#
def load_crop_json(u):
    df = pd.read_json(u, orient='records', lines=True)
    df['crop_type'] = Path(u).name.split("_")[0]
    return df


def is_truthy(value):
    if value:
        return True
    else:
        return False


def unique_by_geohash(df):
    df = df.copy()
    lonlats = df.groupby(c.UNIQUE_GEOHASH).first()[LL]
    return df.merge(lonlats, how='inner', on=LL)


def get_geohash(row, precision):
    return geohash.encode(
        latitude=row.lat,
        longitude=row.lon,
        precision=precision)


def remove_coord_array_nans(row):
    return utils.filter_list_valued_columns(
        row=row,
        test=utils.infinite_along_axis,
        coord_col=c.DATE_COLUMN,
        data_cols=landsat.HARMONIZED_BANDS)


def years_per_geohash(df, min_years=c.MIN_REQUIRED_YEARS):
    df = df.copy()
    gh_count = df.groupby(c.UNIQUE_GEOHASH).size().to_frame('nb_years').reset_index()
    df = df.merge(gh_count, on=c.UNIQUE_GEOHASH, how='inner')
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
# 1. Load and Concatenate (nd) JSONs from GCP
URLS = gcp.gcs_list(
    c.GCS_BUCKET,
    f'{c.GCS_ROOT_FOLDER}/{c.SRC_GCS_BIOMASS_YIELD}',
    search=SEARCH,
    prefix=c.URL_PREFIX)
print(f'- {len(URLS)} urls found')
df = pd.concat([load_crop_json(u) for u in URLS])
if DEV_NB_SAMPLES:
    df = df.sample(DEV_NB_SAMPLES)
print(f'- raw shape: {df.shape}')


print('process data:')
# 2. Remove missing band values (tested using green only)
df = df[~df.green.isna()].copy()
green_exists = df.green.apply(is_truthy)
df = df[green_exists].copy()
print(f'- remove-empty shape:', df.shape)


# 3. Requrie unique lon-lat per geohash-7
df = unique_by_geohash(df)
print(f'- gh-7 unique shape:', df.shape)


# 4. remove nan/none values from coord-arrays
df[LIST_COLUMNS] = df.apply(remove_coord_array_nans, axis=1, result_type='expand')
print(f'- remove-empty shape:', df.shape)


# 5. require `c.MIN_REQUIRED_YEARS` per geohash
df = years_per_geohash(df, min_years=10)
print(f'- min-years shape:', df.shape)


# 6. Add County/State Data
us_gdf = gpd.read_file(paths.local(c.SRC_LOCAL_US_COUNTIES_SHP))
us_gdf = us_gdf.to_crs(epsg=4326)
df = merge_county_data(df, us_gdf)
print(f'merge-county shape:', df.shape)


# 7. Save local and GCS results as line-deliminated JSON
local_dest = paths.local(
    c.DEST_LOCAL_FOLDER,
    c.DEST_BIOMASS_YIELD_NAME)
gcs_dest = paths.gcs(
    c.DEST_GCS_FOLDER,
    c.DEST_BIOMASS_YIELD_NAME)
uri = gcp.save_ld_json(
    df,
    local_dest=local_dest,
    gcs_dest=gcs_dest,
    dry_run=DRY_RUN)
print('[complete]\n\n')
