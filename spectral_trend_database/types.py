from typing import Callable, Union, Optional, Literal, TypeAlias, Sequence
import numpy as np
import dask.array
import xarray as xr


#
# DATA UNION TYPES
#
XR: TypeAlias = Union[xr.Dataset, xr.DataArray]
NPXR: TypeAlias = Union[XR, np.ndarray]
NPD: TypeAlias = Union[np.ndarray, dask.array.Array]
NPDXR_ARRAY: TypeAlias = Union[xr.DataArray, NPD]
NPDXR: TypeAlias = Union[XR, NPD]


#
# LITERAL OPTION TYPES
#
CONV_MODE: TypeAlias = Literal['same', 'valid', 'full']
FILL_METHOD: TypeAlias = Literal[
    'nearest',
    'pad',
    'ffill',
    'backfill',
    'bfill']
INTERPOLATE_METHOD: TypeAlias = Literal[
    'linear',
    'nearest',
    'nearest-up',
    'zero',
    'slinear',
    'quadratic',
    'cubic',
    'previous',
    'next']
XR_INTERPOLATE_METHOD: TypeAlias = Literal[
    'linear',
    'nearest',
    'zero',
    'slinear',
    'quadratic',
    'cubic',
    'polynomial',
    'barycentric',
    'krogh',
    'pchip',
    'spline',
    'akima']


#
# REPEATED ARG TYPES
#
ARGS_KWARGS: TypeAlias = Union[Sequence, dict, Literal[False], None]
STRINGS: TypeAlias = Union[str, Sequence[Union[str, None]]]
EWM_INITALIZER: TypeAlias = Union[
    Literal['sma'],
    Literal['mean'],
    float,
    list,
    np.ndarray,
    Callable,
    Literal[False]]
