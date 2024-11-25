from typing import Callable, Union, Optional, Literal, TypeAlias, Sequence, Any
import typing
import numpy as np
import dask.array
import xarray as xr
from google.cloud import bigquery as bq


#
# HELPERS
#
def type_args(dtype) -> list:
    return list(typing.get_args(dtype))


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
MAP_METHOD: TypeAlias = Literal[
    'sequential',
    'threadpool',
    'pool']
JOINS: TypeAlias = Literal[
    'LEFT',
    'RIGHT',
    'INNER',
    'OUTER',
    'left',
    'right',
    'inner',
    'outer']
CONV_MODE_ARGS: list[Any] = type_args(CONV_MODE)
FILL_METHOD_ARGS: list[Any] = type_args(FILL_METHOD)
INTERPOLATE_METHOD_ARGS: list[Any] = type_args(INTERPOLATE_METHOD)
XR_INTERPOLATE_METHOD_ARGS: list[Any] = type_args(XR_INTERPOLATE_METHOD)
MAP_METHOD_ARGS: list[Any] = type_args(MAP_METHOD)


#
# REPEATED ARG TYPES
#
DICTABLE = Union[dict, bq.table.Row]
PATH_PARTS = Union[str, int, None, Literal[False]]
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
