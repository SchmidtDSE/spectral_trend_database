from typing import Callable, Union, Optional, Literal, TypeAlias, Sequence
import numpy as np
import dask
import xarray as xr


#
# DATA UNION TYPES
#
NPDASK: TypeAlias = Union[np.ndarray, dask.array.Array]
XR: TypeAlias = Union[xr.Dataset, xr.DataArray, dask.array.Array]
NPXR_ARRAY: TypeAlias = Union[xr.DataArray, np.ndarray]
NPXR: TypeAlias = Union[XR, np.ndarray]


ARGS: TypeAlias = Union[Sequence, dict, Literal[False], None]
VARS: TypeAlias = Union[str, Sequence[Union[str, None]]]


EWM_INITALIZER: TypeAlias = Union[
    Literal['sma'],
    Literal['mean'],
    float,
    list,
    np.ndarray,
    Callable,
    Literal[False]]

FILL_METHOD: TypeAlias = Literal[
    'nearest',
    'pad',
    'ffill',
    'backfill',
    'bfill']



CONV_MODE: TypeAlias = Union[
    Literal['same'],
    Literal['valid'],
    Literal['full']]
