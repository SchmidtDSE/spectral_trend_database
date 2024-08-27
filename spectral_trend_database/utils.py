""" utility methods

License:
    BSD, see LICENSE.md
"""
from typing import Any, Union
import pandas as pd
import xarray as xr
import yaml
import crop_yield_database.constants as c


#
# I/O
#
def read_yaml(path: str, *key_path: str) -> Any:
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


#
# PRINTING/LOGGING
#
def message(value: Any, *args: str, level: str='info'):
    assert level in c.INFO_TYPES
    msg = f'[{level}] {c.ROOT_MODULE}'
    for arg in args:
        msg += f'.{arg}'
    msg += f': {value}'
    print(msg)
