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


#
# CONSTANTS
#
CDL = ee.ImageCollection("USDA/NASS/CDL")
# samples_ic = load-samples-here---maybe-ic

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


def to_feat(sample):
	geom = ee.Geometry.Point([sample['lon'], sample['lat']])
	return ee.Feature(geom, sample)


df = pd.read_json(SRC_PATH, lines=True)
samples = df.to_dict('records')[:2]
crop_data = []

for year in YEARS:
	cdl = cdl_for_year(year)
	crop_data += mproc.map_with_threadpool(
		lambda r: value_at_point(r, cdl, year),
		samples)

print(crop_data)


