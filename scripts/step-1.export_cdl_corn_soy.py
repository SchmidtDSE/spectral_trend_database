""" EXPORT CDL CORN-SOY-OTHER

authors:
    - name: Brookie Guzder-Williams

affiliations:
    - University of California Berkeley,
      The Eric and Wendy Schmidt Center for Data Science & Environment

note:
    all pts are "pure" corn or soy (neighborhood radius = 60)
    for at least 15 of 20 years from 2000-2020, so we will trust
    centroid value.

steps:

    1. Load Sample Points
    ...

outputs:

    cdl data [()]:
        - local:
        - gcs:

License:
    BSD, see LICENSE.md
"""
import ee
ee.Initialize()
import pandas as pd
import mproc
from spectral_trend_database.config import config as c
from spectral_trend_database import paths
from spectral_trend_database import gcp
from spectral_trend_database import interface
from spectral_trend_database import utils


#
# CONFIG
#
YEARS = range(c.YEARS[0], c.YEARS[1] + 1)

CS_VALUES = [1, 5]
CORN_VALUE = 0
SOY_VALUE = 1
OTHER_VALUE = 2
REMAP_CS_VALUES = [CORN_VALUE, SOY_VALUE]
MAX_PROCESSES = 4  # low for read-requests
MAP_METHOD = mproc.map_with_threadpool

SRC_PATH = paths.gcs(
    c.SAMPLES_FOLDER,
    c.SAMPLE_POINTS_TABLE_NAME,
    ext='json')


#
# CONSTANTS/DATA
#
CDL = ee.ImageCollection("USDA/NASS/CDL")


# TODO QUERY DB?
SAMPLES = pd.read_json(SRC_PATH, lines=True)
SAMPLES = SAMPLES.to_dict('records')[:c.LIMIT]


#
# METHODS
#
def cdl_for_year(year):
    year = ee.Number(year).toInt()
    start = ee.Date.fromYMD(year, 1, 1)
    end = start.advance(1, 'year')
    data_filter = ee.Filter.date(start, end)
    cdl_year = CDL.filter(data_filter).first()
    return cdl_year.remap(
        CS_VALUES,
        REMAP_CS_VALUES,
        OTHER_VALUE).toInt()


#
# RUN
#
print('EXPORTING CDL (corn/soy/other) FOR', YEARS)
print('- nb_samples', len(SAMPLES))
for year in YEARS:
    print('-', year, '...')
    # 1. process paths
    table_name, local_dest, gcs_dest = interface.table_name_and_paths(
        c.CROP_TYPE_FOLDER,
        table_name=c.CROP_TYPE_TABLE_NAME,
        year=year)

    # 2. get data for year
    cdl = cdl_for_year(year)

    # 3. run
    crop_data = MAP_METHOD(
        lambda row: interface.process_cdl_row(
            row=row,
            im=cdl,
            year=year),
        SAMPLES,
        max_processes=MAX_PROCESSES)
    crop_data = pd.DataFrame(crop_data).sort_values(['year', 'sample_id'])
    print(f'exporting yield data [{crop_data.shape}]:')

    # 4. save data (local, gcs, bq)
    local_dest = utils.dataframe_to_ldjson(
        crop_data,
        dest=local_dest,
        dry_run=c.DRY_RUN)
    interface.save_to_gcp(
        src=local_dest,
        gcs_dest=gcs_dest,
        dataset_name=c.DATASET_NAME,
        table_name=table_name,
        remove_src=True,
        dry_run=c.DRY_RUN)
