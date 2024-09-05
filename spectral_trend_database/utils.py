""" utility methods

License:
    BSD, see LICENSE.md
"""
from typing import Any, Union, Optional
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
        exclude: list[str] = []) -> xr.Dataset:
    """ converts a pd.Series/row of pd.DataFrame to an xr.Dataset

    Creates a Dataset whose data_var values are given by <data_vars> keys, paramatrized by
    coordinate <coord>.  All other values in <row> are added as attributes unless they are
    contained in <exclude>

    Args:
        row (pd.Series): series containing coordinate, data_vars, and attributes
        coord (str): key for coordinate value
        data_vars (list[str]): list of keys for data_vars values
        exclude (list[str] = []): list of keys to exclude from attributes.

    Returns:
        xr.Dataset
    """
    exclude = exclude + data_vars + [coord]
    attrs = {v: row[v] for v in row.keys() if v not in exclude}
    data_var_dict = {v: ([coord], row[v]) for v in data_vars}
    return xr.Dataset(data_vars=data_var_dict, coords={coord: (coord, row[coord])}, attrs=attrs)


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
