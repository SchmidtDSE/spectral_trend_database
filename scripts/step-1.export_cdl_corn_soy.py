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


#
# CONFIG
#
DRY_RUN = False  # TODO: CONFIG OR CML ARG
CS_VALUES = [1, 5]
CORN_VALUE = 0
SOY_VALUE = 1
OTHER_VALUE = 2
REMAP_CS_VALUES = [CORN_VALUE, SOY_VALUE]
SRC_PATH = paths.gcs(
    c.RAW_GCS_FOLDER,
    c.SAMPLE_POINTS_TABLE_NAME,
    ext='json')
YEARS = range(2000, 2022+1)
LIMIT = None


#
# CONSTANTS/DATA
#
CDL = ee.ImageCollection("USDA/NASS/CDL")
LOCAL_DEST_ROOT = paths.local(
    c.CROP_TYPE_LOCAL_FOLDER,
    c.CROP_TYPE_TABLE_NAME)
GCS_DEST_ROOT = paths.gcs(
    c.CROP_TYPE_GCS_FOLDER,
    c.CROP_TYPE_TABLE_NAME)
SAMPLES = pd.read_json(SRC_PATH, lines=True)
SAMPLES = SAMPLES.to_dict('records')[:LIMIT]


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


def value_at_point(row, im, year):
	lon = row['lon']
	lat = row['lat']
	crop_label = im.rename('crop_label').reduceRegion(
		reducer=ee.Reducer.firstNonNull(),
		geometry=ee.Geometry.Point([lon, lat]),
		scale=30).get('crop_label').getInfo()
	if crop_label is None:
		crop_type = 'na'
	elif crop_label == 0:
		crop_type = 'corn'
	elif crop_label == 1:
		crop_type = 'soy'
	else:
		crop_type = 'other'
	return dict(
		sample_id=row['sample_id'],
		year=year,
		crop_label=crop_label,
		crop_type=crop_type)


#
# RUN
#
print('EXPORTING CDL (corn/soy/other) FOR', YEARS)
print('- nb_samples', len(SAMPLES))
for year in YEARS:
	print('-', year, '...')
	cdl = cdl_for_year(year)
	crop_data = mproc.map_with_threadpool(
		lambda r: value_at_point(r, cdl, year),
		SAMPLES)
	crop_data = pd.DataFrame(crop_data)
	print(f'exporting yield data [{crop_data.shape}]:')
	uri = gcp.save_ld_json(
	    crop_data,
	    local_dest=f'{LOCAL_DEST_ROOT}-{year}.json',
	    gcs_dest=f'{GCS_DEST_ROOT}-{year}.json',
	    dry_run=DRY_RUN)


