""" utility methods

License:
    BSD, see LICENSE.md
"""
from typing import Any, Union, Optional, Callable, Iterable, Sequence
from pathlib import Path
from copy import deepcopy
import pandas as pd
import numpy as np
import xarray as xr
import dask.array
import yaml
from spectral_trend_database import constants
from spectral_trend_database import types


#
# I/O
#
def read_yaml(path: str, *key_path: str, safe: bool = False) -> Any:
    """ Reads (and optionally extracts part of) yaml file

    Usage:

    ```python
    data = read_yaml(path)
    data_with_key_path = read_yaml(path,'a','b','c')
    data['a']['b']['c'] == data_with_key_path # ==> True
    ```

    Args:
        path (str): path to yaml file
        *key_path (*str): key-path to extract

    Returns:
        dictionary, or data extracted, from yaml file
    """
    if not safe or Path(path).is_file():
        with open(path, 'rb') as file:
            obj = yaml.safe_load(file)
        for k in key_path:
            obj = obj[k]
        return obj


#
# PD/XR/NP
#
def pandas_to_xr(
        row: pd.Series,
        coord: str,
        data_vars: list[str],
        coord_type: Optional[str] = constants.DATETIME_NS,
        exclude: list[str] = []) -> xr.Dataset:
    """ converts a pd.Series/row of pd.DataFrame to an xr.Dataset

    Creates a Dataset whose data_var values are given by <data_vars> keys, paramatrized by
    coordinate <coord>.  All other values in <row> are added as attributes unless they are
    contained in <exclude>

    Args:
        row (pd.Series): series containing coordinate, data_vars, and attributes
        coord (str): key for coordinate value
        data_vars (list[str]): list of keys for data_vars values
        coord_type (Optional[str] = c.DATETIME_NS):
            if passed coord_array will be cast to <coord_type>. used to avoid
            'non-nanosecond precision' warning from xarray
        exclude (list[str] = []): list of keys to exclude from attributes.

    Returns:
        xr.Dataset
    """
    data_vars = [ d for d in data_vars if d not in [coord]+exclude ]
    attr_exclude = list(exclude) + data_vars + [coord]
    coord_array = row[coord]
    if coord_type:
        coord_array = np.array(coord_array).astype(coord_type)
    attrs = {v: row[v] for v in row.keys() if v not in attr_exclude}
    data_var_dict = {v: ([coord], row[v]) for v in data_vars}
    return xr.Dataset(data_vars=data_var_dict, coords={coord: (coord, coord_array)}, attrs=attrs)


def xr_to_row(
        dataset: xr.Dataset,
        data_vars: Optional[Sequence[str]] = None,
        exclude: Sequence[str] = [],
        as_pandas: bool = True) -> Union[dict, pd.Series]:
    """ transfor xr.dataset to dict or pd.series
    Args:
        row (pd.Series): series containing coordinate, data_vars, and attributes
        data_vars (list[str]):
            list of keys for data_vars values. if None use all data_vars
        exclude (list[str] = []): list of keys to exclude from attributes.
        as_pandas (bool = True): if true return pd.Series, else return dict

    Returns:
        dict or pd.series with <dataset> attrs, coords, and data_vars as key/values
    """
    data = deepcopy(dataset.attrs)
    coords = deepcopy(dataset.coords)
    for coord in coords:
        data[coord] =  coords[coord].data
    if data_vars is None:
        data_vars = list(dataset.data_vars)
    data_vars = [d for d in data_vars if d not in exclude]
    for var in data_vars:
        data[var] =  dataset.data_vars[var].data
    if as_pandas:
        data = pd.Series(data)
    return data


def xr_coord_name(
        data: types.XR,
        data_var: Optional[str] = None) -> str:
    """ extract coord-name from xr data
    Args:
        data (types.XR): xr data
        data_var (Optional[str] = None): name of data_var (only use if <data> is xr.Dataset)

    Returns:
        (str) coord name
    """
    if data_var:
        data = data[data_var]
    return str(list(data.coords)[0])


def npxr_shape(
        data: types.NPDXR,
        data_var: Optional[str] = None,
        data_var_index: Optional[int] = 0) -> tuple:
    """ convience method for determining shape of ndarray, dataset, or data_array
    """
    if isinstance(data, (np.ndarray, dask.array.Array)):
        return data.shape
    else:
        if data_var is None:
            assert isinstance(data_var_index, int)
            data_var = list(data.data_vars)[data_var_index]
        return data[data_var].shape


def dataset_to_ndarray(
        data: xr.Dataset,
        data_vars: Optional[Sequence[str]] = None,
        exclude: Optional[Sequence[str]] = None) -> np.ndarray:
    """ converts xr.dataset to ndarray of <data>.data_vars values

    Args:
        data (xr.Dataset): dataset to extract array
        data_vars (Optional[Sequence[str]] = None):
            list of data_var names to include. if None all data_vars will be used
        exclude (Optional[Sequence[str]] = None):
            list of data_var names to exclude.

    Returns:
        numpy array extracted from xr dataset
    """
    if not data_vars:
        data_vars = list(data.data_vars)
    if exclude:
        data_vars = [v for v in data_vars if v not in exclude]
    return np.vstack([data[v].data for v in data_vars])


def replace_dataset_values(
        data: xr.Dataset,
        values: types.NPD,
        data_vars: Optional[Sequence[str]] = None,
        rename: dict[str, str] = {}) -> xr.Dataset:
    """ """
    if data_vars is None:
        data_vars = list(data.data_vars)
    assert isinstance(data_vars, list)
    for i, dvar in enumerate(data_vars):
        data[dvar].data = values[i]
    if rename:
        rename = {k: v for  (k,v) in rename.items() if k in data_vars}
        data = data.rename(rename)
    return data


def to_ndarray(
        data: types.NPDXR,
        data_vars: Optional[Sequence[str]] = None,
        exclude: Optional[Sequence[str]] = None) -> types.NPD:
    """ convience method for converting data to ndarray

    Args:
        data (types.NPXR): dataset to extract array
        data_vars (Optional[Sequence[str]] = None):
            (xr.dataset only) list of data_var names to include. if None all data_vars will be used
        exclude (Optional[Sequence[str]] = None):
            (xr.dataset only) list of data_var names to exclude.

    Returns:
        numpy array extracted from xr data, or original np/dask array
    """
    if isinstance(data, xr.Dataset):
        data = dataset_to_ndarray(data, data_vars=data_vars, exclude=exclude)
    elif isinstance(data, xr.DataArray):
        data = data.data
    assert isinstance(data, (np.ndarray, dask.array.Array))
    return data


def nan_to_safe_nan(values: Union[list, np.ndarray]) -> np.ndarray:
    """ replace nan values with a "safe" bigquery value

    BigQuery was not working with mixed none/float array valued columns.
    `nan_to_safe_nan` and `safe_nan_to_nan` allow one to transfer back
    and forth between np.nan and constants.SAFE_NAN_VALUE

    """
    values = np.array(values).astype(float)
    values[np.isnan(values)] = constants.SAFE_NAN_VALUE
    return values


def safe_nan_to_nan(values: Union[list, np.ndarray]) -> np.ndarray:
    """ replace "safe" bigquery value with nan

    BigQuery was not working with mixed none/float array valued columns.
    `nan_to_safe_nan` and `safe_nan_to_nan` allow one to transfer back
    and forth between np.nan and constants.SAFE_NAN_VALUE

    """
    values = np.array(values).astype(float)
    values[values == constants.SAFE_NAN_VALUE] = np.nan
    return values


def infinite_along_axis(arr: np.ndarray, axis: int = 0):
    """

    Convience wrapper of np.isfinite, that negates to
    find infinite values along axis. Note that this finds np.nans,
    and if arr -> arr.astype(np.float64) Nones also
    become np.nans.

    Args:
        data (np.ndarray): np.array
        axis (int = 0): axis to check along
    """
    return (~np.isfinite(arr)).any(axis=axis)


def filter_list_valued_columns(
        row: pd.Series,
        test: Callable,
        coord_col: str,
        data_cols: list[str]) -> list[list]:
    """ remove values within array-valued columns

    Args:
        row (pd.Series): series containing <coord_col>, and <data_cols>,
        test (Callable):
            function which takes an array and returns an boolean array with
            True values for data that should be removed and
            False values for data that should remain
        coord_col (str): coordinate array column
        data_cols (list[str]): data array columns

    Returns:
        list of value lists [[coord_values],data_values]
    """
    row = row.copy()
    coord_values = np.array(row[coord_col])
    values = [np.array(v, dtype=np.float64) for v in row[data_cols].values]
    data_values = np.vstack(values, dtype=np.float64)  # type: ignore[call-overload]
    should_be_removed = test(data_values)
    coord_values = coord_values[~should_be_removed].tolist()
    data_values = data_values[:, ~should_be_removed].tolist()
    return [coord_values] + data_values


def cast_duck_array(arr: Iterable, dtype: str = 'str') -> np.ndarray:
    """
    Convience method to cast array. The main purpuse is avoiding
    lambdas in `dataframe.apply(...)`

    Args:
        arr (Iterable): array-like object to cast
        dtype (str = 'str'): dtype to cast to
    Returns:
        numpy array with type <dtype>
    """
    return np.array(arr).astype(dtype)


#
# PRINTING/LOGGING
#
def message(
        value: Any,
        *args: str,
        level: Optional[str] = 'info',
        return_str: bool = False) -> Union[str, None]:
    """ print or return message
    """
    msg = constants.ROOT_MODULE
    if level:
        assert level in constants.INFO_TYPES
        msg = f'[{level}] {msg}'
    for arg in args:
        msg += f'.{arg}'
    msg += f': {value}'
    if return_str:
        return msg
    else:
        print(msg)
        return None
