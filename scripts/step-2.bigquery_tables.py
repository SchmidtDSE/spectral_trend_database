""" BIG QUERY DATASET/TABLE CREATION

authors:
    - name: Brookie Guzder-Williams

affiliations:
    - University of California Berkeley,
      The Eric and Wendy Schmidt Center for Data Science & Environment

steps:

    1. break data into (unique/sorted) subsets and save to GCS
    2. create tables based on subsets saved on GCS

outputs:

    local:

License:
    BSD, see LICENSE.md
"""
from typing import Optional, Union
from pprint import pprint
import pandas as pd
from spectral_trend_database.config import config as c
from spectral_trend_database import gcp
from spectral_trend_database import paths


#
# CONFIG/CONSTANTS
#
DRY_RUN: bool = False
PROJECT: str = None
DATASET_NAME: str = 'BiomassTrends'
LOCATION: str = 'US'
SRC_PATH: str = paths.gcs(
    c.DEST_GCS_FOLDER,
    c.DEST_BIOMASS_YIELD_NAME,
    as_url=True)


SAMPLE_COLS: list[str] = [
    'sample_id',
    'lon',
    'lat',
    'geohash_5',
    'geohash_7',
    'geohash_9',
    'nb_years'
]
ADMIN_COLS: list[str] = [
    'sample_id',
    'STATEFP',
    'COUNTYFP',
    'COUNTYNS',
    'GEOIDFQ',
    'GEOID',
    'NAME',
    'NAMELSAD',
    'STUSPS',
    'STATE_NAME',
    'LSAD',
    'ALAND',
    'AWATER',
]
YIELD_COLS: list[str] = [
    'sample_id',
    'year',
    'crop_type',
    'biomass'
]
LANDSAT_COLS: list[str] = ['sample_id', 'year', 'date'] + c.LSAT_BANDS


#
#  METHODS
#
def save_data_columns(
        df: pd.DataFrame,
        name: str,
        cols: list[str],
        unique_on: Optional[str] = None,
        sort: Optional[Union[str, list[str]]] = None) -> dict:
    local_dest = paths.local(
        c.DEST_LOCAL_FOLDER,
        name)
    gcs_dest = paths.gcs(
        c.DEST_GCS_FOLDER,
        name)
    _df = df[cols]
    if unique_on:
        _df = _df.drop_duplicates(unique_on)
    if sort:
        _df = _df.sort_values(sort)
    uri = gcp.save_ld_json(
        _df,
        local_dest=local_dest,
        gcs_dest=gcs_dest,
        dry_run=DRY_RUN)
    return {'name': name.upper(), 'uri': uri}


#
# RUN
#
print(f'loading data: {SRC_PATH}')
df = pd.read_json(SRC_PATH, orient='records', lines=True)
print('  - shape:', df.shape)
print('save table datasets to gcs:')
TABLE_CONFIGS: list = []
TABLE_CONFIGS.append(save_data_columns(
    df,
    name='samples',
    cols=SAMPLE_COLS,
    unique_on='sample_id',
    sort='sample_id'))
TABLE_CONFIGS.append(save_data_columns(
    df,
    name='administrative_boundaries',
    cols=ADMIN_COLS,
    unique_on='sample_id',
    sort='sample_id'))
TABLE_CONFIGS.append(save_data_columns(
    df,
    name='scym_yield',
    cols=YIELD_COLS,
    sort=['sample_id', 'year']))
TABLE_CONFIGS.append(save_data_columns(
    df,
    name='landsat_raw_masked',
    cols=LANDSAT_COLS,
    sort=['sample_id', 'year']))


print('\n\ncreate tables from datasets:')
pprint(TABLE_CONFIGS)
ds = gcp.load_or_create_dataset(DATASET_NAME, LOCATION)
for config in TABLE_CONFIGS:
    print(config['name'])
    gcp.create_table_from_json(dataset=ds, **config)
print('\n[complete]\n\n')