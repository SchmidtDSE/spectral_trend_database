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
    4. Add `sample_id` (geohash-11)
    5. Convert band-value array-strings to arrays and ensure band-values are not None
    6. require `c.MIN_REQUIRED_YEARS` per geohash
    7. Add County/State Data
    8. Save local and GCS results as line-deliminated JSO

outputs:

    local: c.ROOT_DIR/c.LOCAL_DATA_DIR/c.DEST_LOCAL_FOLDER/c.DEST_NAME
    gcs: gs://c.GCS_BUCKET/c.GCS_ROOT_FOLDER/c.DEST_GCS_FOLDER/c.DEST_NAME

License:
    BSD, see LICENSE.md
"""
import re
from pathlib import Path
import numpy as np
import pandas as pd
import geopandas as gpd  # type: ignore[import-untyped]
import geohash  # type: ignore[import-untyped]
from spectral_trend_database.config import config as c
from spectral_trend_database import gcp
from spectral_trend_database import paths


#
# CONSTANTS
#
DRY_RUN = False  # TODO: CONFIG OR CML ARG
SEARCH = c.SEARCH  # TODO: CONFIG OR CML ARG
COUNTY_ID = 'GEOID'
LL = ['lon', 'lat']
LOCAL_DIR = f'{c.LOCAL_DATA_DIR}/{c.DEST_LOCAL_FOLDER}'


#
# METHODS
#
def load_crop_csv(u):
    df = pd.read_csv(u)
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


def replace_none(values):
    values = np.array(values).astype(float)
    values[np.isnan(values)] = c.NAN_VALUE
    return values


def process_band(value):
    values = process_arr_string(value)
    return replace_none(values)


def process_arr_string(value):
    try:
        v = re.sub('\n', '', value)
        v = re.sub('nan', 'np.nan', v)
        v = re.sub(r'[ \t]+', '', v)
        v = re.sub(r'\[,', '[', v)
        v = re.sub(r'\,]', ']', v)
        return np.array(eval(v))
    except Exception as e:
        print('ERROR:', value, type(value))
        raise e


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


# 1. Load and Concatenate CSVs from GCP
URLS = gcp.gcs_list(
    c.GCS_BUCKET,
    f'{c.GCS_ROOT_FOLDER}/{c.SRC_GCS_BIOMASS_YIELD}',
    search=SEARCH,
    prefix=c.URL_PREFIX)
print(f'- {len(URLS)} found')
df = pd.concat([load_crop_csv(u) for u in URLS])
print(f'- raw shape: {df.shape}')


# 2. Remove missing band values (tested using green only)
df = df[~df.green.isna()].copy()
green_exists = df.green.apply(is_truthy)
df = df[green_exists].copy()
print(f'- remove-empty shape:', df.shape)


# 3. Requrie unique lon-lat per geohash-7
df = unique_by_geohash(df)
print(f'- gh-7 unique shape:', df.shape)


# 4. Add `sample_id` (geohash-11)
df['sample_id'] = df.apply(lambda r: get_geohash(r, 11), axis=1)
print(f'- sample_id shape:', df.shape)


# 5. Convert band-value array-strings to arrays and ensure band-values are not None
print('- convert bands array-strings')
for band in c.LSAT_BANDS:
    print(f'  {band} ...')
    df[band] = df[band].apply(process_band)
print(f'  date ...')
df['date'] = df.date.apply(eval)
print(f'- convert-bands shape:', df.shape)


# 6. require `c.MIN_REQUIRED_YEARS` per geohash
df = years_per_geohash(df, min_years=10)
print(f'- min-years shape:', df.shape)


# 7. Add County/State Data
us_gdf = gpd.read_file(paths.local(c.SRC_LOCAL_US_COUNTIES_SHP))
us_gdf = us_gdf.to_crs(epsg=4326)
df = merge_county_data(df, us_gdf)
print(f'merge-county shape:', df.shape)


# 8. Save local and GCS results as line-deliminated JSO
local_dest = paths.local(c.DEST_NAME)
gcs_dest = paths.gcs(c.DEST_NAME)
print(c.DEST_NAME, local_dest, gcs_dest)
print(f'save [{df.shape}]:')
if DRY_RUN:
    print('- local:', local_dest, '(dry-run)')
else:
    print('- local:', local_dest)
    df.to_json(local_dest, orient='records', lines=True)
if DRY_RUN:
    print('- gcs:', gcs_dest, '(dry-run)')
else:
    print('- gcs:', gcs_dest)
    gcp.upload_file(local_dest, c.GCS_BUCKET, gcs_dest)
print('[complete]\n\n')
