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
import re
import pandas as pd
import xarray as xr
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


# YEARS = range(2003, 2003 + 1)
# LIMIT = 100
# DRY_RUN = False

SRC_TABLE_NAME = c.SMOOTHED_INDICES_TABLE_NAME.upper()
# MAP_METHOD = 'sequential'
MAP_METHOD = 'threadpool'
REPO_PATH = '/Users/brookieguzder-williams/code/dse/COVERCROPS/spectral_trend_database/repo'
LOCAL_DEV_PATH = f'{REPO_PATH}/nb/smoothed-samples.stats.2020.json'
LOCAL_DEV_DATA = False
DATA_VARS_SQL = (
    "SELECT column_name FROM "
    "`dse-regenag.BiomassTrends.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS` "
    "WHERE table_name = 'SMOOTHED_INDICES_V1' AND data_type = 'ARRAY<FLOAT64>'"
)


PRE_START_YYMM = '12-01'
PRE_END_YYMM = '03-15'
POST_START_YYMM = '04-15'
POST_END_YYMM = '11-01'

#
# METHODS
#
def process_row(
        row: types.DICTABLE,
        local_dest: str,
        local_dest_pre: str,
        local_dest_post: str,
        data_vars: list):
    sample_id = row['sample_id']
    year = int(row['year'])
    data = utils.row_to_xr(
        dict(row),
        coord=c.COORD_COLUMN,
        data_vars=data_vars)
    ds_pre = data.sel(dict(date=slice(f'{year-1}-{PRE_START_YYMM}', f'{year}-{PRE_END_YYMM}')))
    ds_post = data.sel(dict(date=slice(f'{year-1}-{POST_START_YYMM}', f'{year}-{POST_END_YYMM}')))
    stats_ds = utils.xr_stats(data, data_vars=data_vars)
    stats_pre_ds = utils.xr_stats(ds_pre, data_vars=data_vars)
    stats_post_ds = utils.xr_stats(ds_post, data_vars=data_vars)
    apped_row(stats_ds, local_dest, sample_id, year)
    apped_row(stats_pre_ds, local_dest_pre, sample_id, year)
    apped_row(stats_post_ds, local_dest_post, sample_id, year)


def apped_row(ds: xr.Dataset, dest: str, sample_id: str, year: int):
    data = dict(sample_id=sample_id, year=year)
    data.update(utils.xr_to_row(ds))
    utils.append_ldjson(file_path=dest, data=data)


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
        table_name=c.INDICES_STATS_TABLE_NAME,
        local_folder=c.INDICES_STATS_FOLDER,
        gcs_folder=c.INDICES_STATS_FOLDER,
        dataset_name=c.DATASET_NAME,
        file_base_name=None)
    local_dest_pre = re.sub(r'json$', f'{PRE_START_YYMM}_{PRE_END_YYMM}.json', local_dest)
    local_dest_post = re.sub(r'json$', f'{POST_START_YYMM}_{POST_END_YYMM}.json', local_dest)
    gcs_dest_pre = re.sub(r'json$', f'{PRE_START_YYMM}_{PRE_END_YYMM}.json', gcs_dest)
    gcs_dest_post = re.sub(r'json$', f'{POST_START_YYMM}_{POST_END_YYMM}.json', gcs_dest)
    table_name_pre = f'{table_name}_OFF_SEASON'
    table_name_post = f'{table_name}_GROWING_SEASON'
    print('\t pre:', local_dest_pre)
    print('\t post:', local_dest_post)
    print('\t pre-gcs:', gcs_dest_pre)
    print('\t post-gcs:', gcs_dest_post)

    # 2. query data
    print('- run query:')
    if LOCAL_DEV_DATA:
        # data = pd.read_json(LOCAL_DEV_PATH, orient='records', lines=True)
        # data = data[[c.COORD_COLUMN] + c.META_COLUMNS + SRC_INDICES]
        # data = data.to_dict('records')
        raise NotImplementedError
    else:
        data = list(query.run(
            table=SRC_TABLE_NAME,
            year=year,
            limit=LIMIT,
            to_dataframe=False))
    data_vars =  query.run(sql=DATA_VARS_SQL, to_dataframe=False)
    data_vars = [r.column_name for r in data_vars]
    print('\t size:', len(data))
    print('\t data_vars:', data_vars)
    print()

    # 3. run
    errors = runner.mapper(MAP_METHOD)(
        process_row,
        data,
        max_processes=c.MAX_PROCESSES,
        local_dest=local_dest,
        local_dest_pre=local_dest_pre,
        local_dest_post=local_dest_post,
        data_vars=data_vars)


    # 4. gcp
    if DRY_RUN:
        print('- dry_run [skipping gcp]')
    else:
        runner.save_to_gcp(
            local_dest=local_dest,
            gcs_dest=gcs_dest,
            dataset_name=dataset_name,
            table_name=table_name)
        runner.save_to_gcp(
            local_dest=local_dest_pre,
            gcs_dest=gcs_dest_pre,
            dataset_name=dataset_name,
            table_name=table_name_pre)
        runner.save_to_gcp(
            local_dest=local_dest_post,
            gcs_dest=gcs_dest_post,
            dataset_name=dataset_name,
            table_name=table_name_post)

    # 5. report on errors
    runner.print_errors(errors)
    print('\n' * 2)
