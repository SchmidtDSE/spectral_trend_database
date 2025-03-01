""" COMPUTE RAW SPECTRAL INCIDES

authors:
    - name: Brookie Guzder-Williams

affiliations:
    - University of California Berkeley,
      The Eric and Wendy Schmidt Center for Data Science & Environment

steps:

    1. create table for raw spectral indices
    2. for each year (2000-2022):
        - compute all indices
        - save to gcs
        - save as bigquery table

outputs:


runtime: ~ XXX minutes

License:
    BSD, see LICENSE.md
"""
from typing import Optional, Union
from pprint import pprint
import pandas as pd
from spectral_trend_database.config import config as c
from spectral_trend_database import gcp
from spectral_trend_database import paths
from spectral_trend_database import spectral
from spectral_trend_database import query
from spectral_trend_database import utils
from spectral_trend_database import interface
from spectral_trend_database.gee import landsat


#
# CONSTANTS
#
YEARS = range(c.YEARS[0], c.YEARS[1] + 1)
HEADER_COLS = ['sample_id', 'year', 'date'] + landsat.HARMONIZED_BANDS


#
# METHODS
#
def process_raw_indices_for_year(
        df: pd.DataFrame,
        index_config: dict[str, Union[str, dict]]) -> None:
    indices = index_config.get('indices', index_config)
    assert isinstance(indices, dict)
    df = spectral.add_index_arrays(df, indices=indices)
    _data_cols = [n for n in df.columns if n not in HEADER_COLS]
    _data_cols.sort()
    return df[HEADER_COLS + _data_cols]


#
# RUN
#
index_config = spectral.index_config(
    c.DEFAULT_SPECTRAL_INDEX_CONFIG,
    extract_indices=False)
print('\ncompute raw indices:')
pprint(index_config['indices'], indent=4, width=100)
print('-' * 50)


for year in YEARS:
    print(f'\n- year: {year}')
    # 1. process paths
    table_name, local_dest, gcs_dest = interface.table_name_and_paths(
        c.RAW_INDICES_FOLDER,
        table_name=c.RAW_INDICES_TABLE_NAME,
        year=year)

    # 2. load data
    src_uri = paths.gcs(
        c.RAW_LANDSAT_FOLDER,
        f'{c.RAW_LANDSAT_FILENAME}-{year}',
        ext='json')
    print('- src:', src_uri)
    df = pd.read_json(src_uri, lines=True)
    print('- src shape:', df.shape)

    # 3. run
    df = process_raw_indices_for_year(
        df,
        index_config=index_config)

    # save data
    local_dest = utils.dataframe_to_ldjson(
        df,
        dest=local_dest,
        dry_run=c.DRY_RUN)
    interface.save_to_gcp(
        src=local_dest,
        gcs_dest=gcs_dest,
        dataset_name=c.DATASET_NAME,
        table_name=table_name,
        remove_src=True,
        dry_run=c.DRY_RUN)
