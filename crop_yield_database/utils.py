""" utility methods

License:
    BSD, see LICENSE.md
"""
import pandas as pd
import xarray as xr
import yaml


#
# I/O
#
def read_yaml(path: str) -> dict:
    """ read yaml file

    Args:
        path (str): path to yaml file

    Returns:
        a dictionary for extracted yaml
    """
    with open(path, 'rb') as file:
        obj = yaml.safe_load(file)
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
