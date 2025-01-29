""" METHODS AND CONSTANTS FOR ACCESSING LANDSAT THROUGH Google Earth Engine

authors:
    - name: Brookie Guzder-Williams

affiliations:
    - University of California Berkeley,
      The Eric and Wendy Schmidt Center for Data Science & Environment

band descriptions:
    meta:
        min: 1
        max: 65455
        scale: 2.75e-05
        offset: -0.2

    landsat 8:
        SR_B2    0.452-0.512 μm    (blue)
        SR_B3    0.533-0.590 μm    (green)
        SR_B4    0.636-0.673 μm    (red)
        SR_B5    0.851-0.879 μm    (nir)
        SR_B6    1.566-1.651 μm    (swir1)
        SR_B7    2.107-2.294 μm    (swir2)
        ST_B10   (surface-temp K)  (temp)

    landsat 5/7:
        SR_B1    0.45-0.52 μm    (blue)
        SR_B2    0.52-0.60 μm    (green)
        SR_B3    0.63-0.69 μm    (red)
        SR_B4    0.77-0.90 μm    (near infrared)
        SR_B5    1.55-1.75 μm    (swir1)
        SR_B7    2.08-2.35 μm    (swir2)

License:
    BSD, see LICENSE.md
"""
from typing import Union, Optional
import ee


#
# CONSTANTS
#
NOMINAL_SCALE = 30
GRID_DEGREE_SIZE = 0.0439453125
LSAT_SCALE_FACTOR = 0.0000275
LSAT_OFFSET = -0.2
LSAT_ST_SCALE_FACTOR = 0.00341802
LSAT_ST_OFFSET = 149
TRANSFORM = [GRID_DEGREE_SIZE, 0, 0, 0, -GRID_DEGREE_SIZE, 0]
MASK_VALUE = 2.1474836e9
QA_BANDS = ['QA_PIXEL', 'QA_RADSAT']
L8_SR_ID = 'LANDSAT/LC08/C02/T1_L2'
L7_SR_ID = 'LANDSAT/LE07/C02/T1_L2'
L5_SR_ID = 'LANDSAT/LT05/C02/T1_L2'
L8_BANDS = [
    'SR_B2',
    'SR_B3',
    'SR_B4',
    'SR_B5',
    'SR_B6',
    'SR_B7',
    'ST_B10'
]
L57_BANDS = [
    'SR_B1',
    'SR_B2',
    'SR_B3',
    'SR_B4',
    'SR_B5',
    'SR_B7',
    'ST_B6'
]
HARMONIZED_BANDS = [
    'blue',
    'green',
    'red',
    'nir',
    'swir1',
    'swir2',
    'st'
]
SPECTRAL_BANDS = HARMONIZED_BANDS[:-1]
TEMPERATURE_BANDS = HARMONIZED_BANDS[-1:]

MISSIONS: dict[int, dict] = {
    8: {
        'id': L8_SR_ID,
        'bands': L8_BANDS,
        "dates": "2013-03-18T15:58:14 -"
    },
    7: {
        'id': L7_SR_ID,
        'bands': L57_BANDS,
        "dates": "1999-05-28T01:02:17 -"
    },
    5: {
        'id': L5_SR_ID,
        'bands': L57_BANDS,
        'dates': "1984-03-16T16:18:01 - 2012-05-05T17:54:06"
    }
}


#
# METHODS
#
def cloud_masked_rescaled_image(
        im: ee.Image,
        bands: list[str] = HARMONIZED_BANDS,
        positive_optical_mask: bool = True,
        mission: Optional[int] = None) -> ee.Image:
    """ cloud mask/rescaled landsat image
    Args:
        im (ee.Image): landsat image
        bands (list[str]): optical bands to be kept
        positive_optical_mask (bool = True):
            if true masks out pixels where any of the optical bands lte 0.
            ignored if there are no spectral bands in <bands>
        mission (Optional[int]):
            one of 8, 7, 5 (see `MISSIONS` dict above)
            if exists adds `mission` property to ee.image

    Returns:
         (ee.Image) cloud masked, scaled and offset, bands
         maintaining im properties and timestamps.
    """
    _im = ee.Image(im)
    qa_mask = _im.select('QA_PIXEL').bitwiseAnd(0b11111).eq(0)
    saturation_mask = _im.select('QA_RADSAT').eq(0)
    _im = _im.updateMask(qa_mask).updateMask(saturation_mask)
    _spec_band_names = [b for b in bands if b in SPECTRAL_BANDS]
    _therm_band_names = [b for b in bands if b in TEMPERATURE_BANDS]
    _bands = []
    if _spec_band_names:
        _optical_bands = _im.select(_spec_band_names).multiply(LSAT_SCALE_FACTOR).add(LSAT_OFFSET)
        _bands.append(_optical_bands)
    if _therm_band_names:
        _therm_bands =  _im.select(_therm_band_names).multiply(LSAT_ST_SCALE_FACTOR).add(LSAT_ST_OFFSET)
        _bands.append(_therm_bands)
    _im = ee.Image(_bands)
    if _spec_band_names and positive_optical_mask:
        _im = _im.updateMask(_optical_bands.reduce(ee.Reducer.min()).gt(0))
    if mission:
        _im = _im.set('mission', mission)
    _im = _im.set('system:time_start', im.date().millis())
    return ee.Image(_im)


def cloud_masked_rescaled_ic_for_mission(
        mission: int,
        bands: list[str] = HARMONIZED_BANDS,
        data_filter: Optional[ee.Filter] = None) -> ee.ImageCollection:
    """ cloud masked rescaled landsat bands ic for mission
    Args:
        mission (int): one of 8, 7, 5 (see `MISSIONS` dict above)
        data_filter (Optional[ee.Filter]): if exists, filter ic

    Returns:
         (ee.ImageCollection) of cloud masked, scaled and offset, landsat images
    """
    info = MISSIONS[mission]
    ic = ee.ImageCollection(info['id'])
    if data_filter:
        ic = ic.filter(data_filter)
    if bands:
        ic = ic.select(QA_BANDS + info['bands'], QA_BANDS + bands)
    else:
        bands = info['bands']
    ic = ic.map(lambda im: cloud_masked_rescaled_image(im, bands=bands, mission=mission))
    return ic


def harmonized_cloud_masked_rescaled_ic(
        missions: list[int] = [5, 7, 8],
        data_filter: Optional[ee.Filter] = None) -> ee.ImageCollection:
    """ cloud masked rescaled landsat bands ic for mission
    Args:
        mission (int): one of 8, 7, 5 (see `MISSIONS` dict above)
        data_filter (Optional[ee.Filter]): if exists, filter ic

    Returns:
         (ee.ImageCollection) of cloud masked, scaled and offset, landsat images
    """
    ic = cloud_masked_rescaled_ic_for_mission(missions[0], data_filter=data_filter)
    for m in missions[1:]:
        ic = ic.merge(cloud_masked_rescaled_ic_for_mission(m, data_filter=data_filter))
    return ic
