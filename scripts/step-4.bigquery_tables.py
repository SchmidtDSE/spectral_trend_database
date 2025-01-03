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

- local:
    - <root-data-folder>/data/processed/biomass_landsat/sample_points
    - <root-data-folder>/data/processed/biomass_landsat/administrative_boundaries
    - <root-data-folder>/data/processed/biomass_landsat/scym_yield
    - <root-data-folder>/data/processed/biomass_landsat/landsat_raw_masked

- gcs (gs://agriculture_monitoring/spectral_trend_database/v1/processed/biomass_landsat):
    - sample_points
    - administrative_boundaries
    - scym_yield
    - landsat_raw_masked

- bigquery:
     - SAMPLE_POINTS
     - ADMINISTRATIVE_BOUNDARIES
     - SCYM_YIELD
     - LANDSAT_RAW_MASKED

runtime: ~ 15 minutes

License:
    BSD, see LICENSE.md
"""
from typing import Optional, Union
from pprint import pprint
import pandas as pd
from spectral_trend_database.config import config as c
from spectral_trend_database import gcp
from spectral_trend_database import paths
from spectral_trend_database.gee import landsat


#
# CONFIG/CONSTANTS
#
DRY_RUN: bool = False
PROJECT: Optional[str] = None
YEARS = range(2004, 2011 + 1)
TABLE_CONFIGS = [
    dict(
        name=c.SAMPLE_POINTS_TABLE_NAME,
        uri=paths.gcs(
            c.RAW_GCS_FOLDER,
            c.SAMPLE_POINTS_TABLE_NAME,
            ext='json')
    ),
    dict(
        name=c.YIELD_TABLE_NAME,
        uri=paths.gcs(
            c.RAW_GCS_FOLDER,
            c.YIELD_TABLE_NAME,
            ext='json')
    )]


for year in YEARS:
    TABLE_CONFIGS += [
        dict(
            name=c.CROP_TYPE_TABLE_NAME,
            uri=paths.gcs(
                c.CROP_TYPE_GCS_FOLDER,
                f'{c.CROP_TYPE_TABLE_NAME}-{year}',
                ext='json')
        )]


#
#  METHODS
#

#
# RUN
#
print('\n\ncreate tables from datasets:')
ds = gcp.load_or_create_dataset(c.DATASET_NAME, c.LOCATION)
for config in TABLE_CONFIGS:
    config['name'] = config['name'].upper()
    print('-', config['name'])
    print(' ', config['uri'])
    gcp.create_or_update_table_from_json(dataset=ds, **config)
print('\n[complete]\n\n')
