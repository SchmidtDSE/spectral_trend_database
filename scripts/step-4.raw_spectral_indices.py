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


#
# CONSTANTS
#
YEARS = range(2008, 2011 + 1) #  TODO LIM HACK
LIMIT = None


#
# METHODS
#
def remove_coord_array_infinities(row: pd.Series, indices: list[str]):
    return utils.filter_list_valued_columns(
        row=row,
        test=utils.infinite_along_axis,
        coord_col=c.DATE_COLUMN,
        data_cols=indices)


def process_raw_indices_for_year(
        index_config: dict[str, Union[str, dict]],
        year: int,
        query_name: str = c.RAW_LANDSAT_QUERY,
        table_name: Optional[str] = None) -> None:
    indices = index_config.get('indices', index_config)
    assert isinstance(indices, dict)
    if not table_name:
        assert isinstance(index_config['name'], str)
        table_name = index_config['name']
    assert isinstance(table_name, str)
    table_name = table_name.upper()
    index_names = list(indices.keys())
    file_name = f'{table_name.lower()}-{year}.json'
    local_dest = paths.local(
        c.DEST_LOCAL_FOLDER,
        c.RAW_INDICES_FOLDER,
        file_name)
    gcs_dest = paths.gcs(
        c.DEST_GCS_FOLDER,
        c.RAW_INDICES_FOLDER,
        file_name)
    print(f'\n\nquery database [{query_name}, {year}]')
    df = query.run(query_name, year=year, limit=LIMIT)
    print('- shape:', df.shape)
    print('- compute raw indices')
    df = spectral.add_index_arrays(df, indices=indices)
    print('- add indices shape: ', df.shape)
    print(f'- save json [{file_name}]')
    uri = gcp.save_ld_json(
        df,
        local_dest=local_dest,
        gcs_dest=gcs_dest,
        dry_run=False)
    assert isinstance(uri, str)
    print(f'- update table [{c.DATASET_NAME}.{table_name}]')
    gcp.create_or_update_table_from_json(
        gcp.load_or_create_dataset(c.DATASET_NAME, c.LOCATION),
        name=table_name,
        uri=uri)


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
    process_raw_indices_for_year(
        index_config=index_config,
        year=year)
