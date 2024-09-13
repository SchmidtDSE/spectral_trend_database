""" Helper methods for Earth Engine

License:
    BSD, see LICENSE.md
"""
from typing import Union, Optional
import ee
import re
from pprint import pprint
import requests
import numpy as np
import xarray as xr


EE_CRS = 'EPSG:3857'
MASK_VALUES = [2.1474836e9, 0, 2**16 - 1]
MASK_VALUE_BAND = 'blue'


#
# XR
#
def get_ee_xrr(
        ic: ee.ImageCollection,
        geom: ee.Geometry,
        scale: float,
        attrs: Optional[dict] = None,
        mask_values: Optional[list[Union[int, float]]] = MASK_VALUES,
        mask_value_band: Optional[str] = MASK_VALUE_BAND,
        load: bool = True):
    """ get ee data using xarray
    Args:
        ic (ee.ImageCollection):
        scale (float): resolution
        geom (ee.Geometry):
        attrs (dict): attributes-dict to add to xr-dataset
        mask_value (float): set <mask_value> pixels to nan
        mask_value_band (str):
            name of band (or dataset data_var) to check for <mask_value>
        load (bool): if true load dataset

    Returns:
        data as xarray
    """
    ds = xr.open_dataset(
        ic,  # type: ignore[arg-type]
        scale=scale,
        engine='ee',
        crs=EE_CRS,
        geometry=geom,
        mask_and_scale=True,
        cache=True)
    ds = ds.chunk().sortby('time')
    if load:
        ds = ds.load()
    if mask_values is not None:
        test = ~np.isin(ds[mask_value_band].data, mask_values)
        ds = ds.where(test, np.nan)
    if attrs:
        ds = ds.assign_attrs(attrs)
    return ds


#
# CORE
#
def safe_init(quiet: bool = False):
    """ Safely initialize earth-engine

    Warns, but does not throw an exception on connection-error.
    This is useful for modules that in part use gee, but you want
    to be able to load/run offline

    Returns:
        True if initialized otherwised False
    """
    try:
        ee.Initialize()
        return True
    except requests.exceptions.ConnectionError as e:
        if not quiet:
            print('Failed to Initialize Earth Engine:', e)
        return False


def get_info(*args, **kwargs):
    """ Convinece method for getting python values of ee.Objects

    Works with a single call of `.getInfo()` instead of a call
    per key/value-pair.

    Usage:

    ```python
    data = get_info(
        crs=crs,
        tile=tile,
        bands=s2_median.bandNames(),
        nb_s2_images=S2.size())
     ```

    Args:
        *args: Variable length argument list.
        **kwargs: Arbitrary keyword arguments.

    Returns:
        a dictionary or list of python objects
    """
    if kwargs:
        if args:
            kwargs['__ARGS__'] = args
        return ee.Dictionary(kwargs).getInfo()
    else:
        if (len(args) == 1) and isinstance(args, tuple):
            args = args[0]
            if re.search(r'ee\.', str(type(args))):
                return args.getInfo()
            else:
                print('[utils.ee] WARNING: get_info called on non-ee object')
                return args
        return ee.List(args).getInfo()


def print_info(*args, **kwargs):
    """ Convinece method for printing python values of ee.Objects

    Works with a single call of `.getInfo()` instead of a call
    per key/value-pair.

    Usage:

    ```python
    print_info(
        crs=crs,
        tile=tile,
        bands=s2_median.bandNames(),
        nb_s2_images=S2.size())
    ```

    Args:
        *args: Variable length argument list.
        **kwargs: Arbitrary keyword arguments.
    """
    pprint(get_info(*args, **kwargs))
