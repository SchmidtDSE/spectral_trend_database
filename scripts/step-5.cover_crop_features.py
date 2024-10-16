""" COVER CROP FEATURES

authors:
    - name: Brookie Guzder-Williams

affiliations:
    - University of California Berkeley,
      The Eric and Wendy Schmidt Center for Data Science & Environment

steps:

    for smoothed daily indices ...
    compute:
        macd(div) features

outputs:


runtime: ~ XXX minutes

License:
    BSD, see LICENSE.md
"""
import pandas as pd
from spectral_trend_database.config import config as c
from spectral_trend_database import query
from spectral_trend_database import smoothing
from spectral_trend_database import utils
from spectral_trend_database import runner
from spectral_trend_database import types


#
# CONSTANTS
#
YEARS = range(2020, 2020 + 1)
LIMIT = 2
DRY_RUN = False
SRC_INDICES = ['ndvi', 'evi', 'evi2']
MACD_PARTS = [
    'ema_a',
    'ema_b',
    'macd',
    'ema_c',
    'macd_div']


# YEARS = range(2003, 2003 + 1)
# LIMIT = 100
# DRY_RUN = False

TABLE_NAME = c.SMOOTHED_INDICES_TABLE_NAME.upper()
# MAP_METHOD = 'sequential'
MAP_METHOD = 'threadpool'
REPO_PATH = '/Users/brookieguzder-williams/code/dse/COVERCROPS/spectral_trend_database/repo'
LOCAL_DEV_PATH = f'{REPO_PATH}/nb/smoothed-sample.2020.json'
LOCAL_DEV_DATA = False


#
# METHODS
#
def process_row(data: types.DICTABLE, local_dest: str, data_vars: list, list_vars: list):
    _data = utils.row_to_xr(
        dict(data),
        coord=c.COORD_COLUMN,
        data_vars=data_vars)
    _data = smoothing.macd_processor(_data, spans=[5, 10, 5])
    _data = utils.xr_to_row(_data)
    _data = runner.post_process_row(_data, list_vars=list_vars)
    utils.append_ldjson(file_path=local_dest, data=_data)


#
# RUN
#
print('\n' * 2)
print('compute macd(-div) series:')
print('=' * 100)
for year in YEARS:
    print('-' * 100)
    # 1. process paths
    table_name, dataset_name, local_dest, gcs_dest = runner.destination_strings(
        year,
        table_name=c.MACD_TABLE_NAME,
        local_folder=c.MACD_FOLDER,
        gcs_folder=c.MACD_FOLDER,
        dataset_name=c.DATASET_NAME,
        file_base_name=None)

    # 2. query data
    print('- run query:')
    if LOCAL_DEV_DATA:
        data = pd.read_json(LOCAL_DEV_PATH, orient='records', lines=True)
        data = data[[c.COORD_COLUMN] + c.META_COLUMNS + SRC_INDICES]
        data = data.to_dict('records')
    else:
        data = list(query.run(
            table=c.SMOOTHED_INDICES_TABLE_NAME,
            table_config={
                'select': ','.join(
                    [c.COORD_COLUMN] + c.META_COLUMNS + SRC_INDICES)
            },
            year=year,
            limit=LIMIT,
            to_dataframe=False))
    data_vars = runner.get_data_vars(data[0])
    list_vars = utils.list_prefixes(SRC_INDICES, MACD_PARTS)
    print('\t size:', len(data))
    print('\t data_vars:', data_vars)
    print('\t list_vars:', list_vars)
    print()

    # 3. run
    errors = runner.mapper(MAP_METHOD)(
        process_row,
        data,
        max_processes=c.MAX_PROCESSES,
        local_dest=local_dest,
        data_vars=data_vars,
        list_vars=list_vars)

    # 4. gcp
    if DRY_RUN:
        print('- dry_run [skipping gcp]')
    else:
        runner.save_to_gcp(
            local_dest=local_dest,
            gcs_dest=gcs_dest,
            dataset_name=dataset_name,
            table_name=table_name)

    # 5. report on errors
    runner.print_errors(errors)
    print('\n' * 2)
