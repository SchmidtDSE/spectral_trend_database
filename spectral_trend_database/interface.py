import ee
ee.Initialize()
import pandas as pd


#
# CONSTANTS
#
CORN_TYPE: str = 'corn'
SOY_TYPE: str = 'soy'
OTHER_TYPE: str = 'other'
NA_TYPE: str = 'na'

CORN_LABEL: int = 0
SOY_LABEL: int = 1
OTHER_LABEL: int = 2
NA_LABEL: int = 3


#
# METHODS
#
def process_cdl_row(row: pd.Series, im: ee.Image, year: int):
    lon = row['lon']
    lat = row['lat']
    crop_label = im.rename('crop_label').reduceRegion(
        reducer=ee.Reducer.firstNonNull(),
        geometry=ee.Geometry.Point([lon, lat]),
        scale=30).get('crop_label').getInfo()
    if crop_label is None:
        crop_type = NA_TYPE
        crop_label = NA_LABEL
    elif crop_label == CORN_LABEL:
        crop_type = CORN_TYPE
    elif crop_label == SOY_LABEL:
        crop_type = SOY_TYPE
    else:
        crop_label = OTHER_LABEL
        crop_type = OTHER_TYPE
    return dict(
        sample_id=row['sample_id'],
        year=year,
        crop_label=crop_label,
        crop_type=crop_type)
