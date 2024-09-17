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


#
# LITERAL OPTION TYPES
#
FILL_METHOD: TypeAlias = Literal['nearest','pad','ffill','backfill','bfill']
CONV_MODE: TypeAlias = Literal['same','valid','full']


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
