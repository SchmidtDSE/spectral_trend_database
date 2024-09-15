""" utility methods

License:
    BSD, see LICENSE.md
"""
from typing import Any, Union, Optional, Callable
from pathlib import Path
import pandas as pd
import numpy as np
import xarray as xr
import yaml
from spectral_trend_database import constants


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
    exclude = exclude + data_vars + [coord]
    coord_array = row[coord]
    if coord_type:
        coord_array = coord_array.astype(coord_type)
    attrs = {v: row[v] for v in row.keys() if v not in exclude}
    data_var_dict = {v: ([coord], row[v]) for v in data_vars}
    return xr.Dataset(data_vars=data_var_dict, coords={coord: (coord, coord_array)}, attrs=attrs)


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
