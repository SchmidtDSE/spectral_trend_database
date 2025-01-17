""" methods for computing spectral indices

License:
    BSD, see LICENSE.md
"""
from typing import Optional
import re
import pandas as pd
import numpy as np
from spectral_trend_database.config import config as c
from spectral_trend_database import utils
from spectral_trend_database.gee import landsat


#
# CONSTANTS
#
ID_COLUMNS = ['sample_id', 'date', 'year']


#
# METHODS
#
def index_config(
        name: str = c.DEFAULT_SPECTRAL_INDEX_CONFIG,
        extract_indices: bool = True) -> dict:
    """ load spectral index config

    convinence wrapper of `_indicesutils.read_yaml`

    Args:

        name (str = c.DEFAULT_SPECTRAL_INDEX_CONFIG):
            name of, or path to, config file.
            if re.search(r'(yaml|yml)$', <name>) loads yaml file with at path at <name>
            else load yaml at '<project-root>/config/spectral_indices/<name>.yaml'
        extract_indices (bool=True): if true extract the `[indices]` from the yaml file.

    Returns:

       spectral index config
    """
    if not re.search(r'(yaml|yml)$', name):
        name = f'{c.SPECTRAL_INDEX_DIR}/{name}.yaml'
    config = utils.read_yaml(name)
    if extract_indices:
        config = config['indices']
    return config


def index_arrays(
        row: pd.Series,
        indices: dict[str, str],
        bands: list[str] = landsat.HARMONIZED_BANDS,
        coord: str = 'date') -> list[np.ndarray]:
    """ computes spectral indices
    TODO: REMOVE/ARCHVIVE

    Computes spectral indices from a pd.Series (dataframe row) of band-valued
    timeseries

    Args:

        row (pd.Series): series containing spectral band and coord arrays
        indices (dict[str, str]): config containing spectral-index equations
        bands (list[str]=landsat.HARMONIZED_BANDS): list of spectral band nanmes used in equations
        coord (str='date'): key for coordinate column

    Returns:

        (list[np.ndarray]) of spectral index values
    """
    ds = utils.row_to_xr(row, coord=coord, data_vars=bands)
    index_datasets = ds.eval(list(indices.values()))  # type: ignore[arg-type]
    index_arrays = [v.data for v in index_datasets]  # type: ignore[attr-defined]
    return index_arrays


def add_index_arrays(
        data: pd.DataFrame,
        name: Optional[str] = c.DEFAULT_SPECTRAL_INDEX_CONFIG,
        indices: Optional[dict[str, str]] = None,
        bands: list[str] = landsat.HARMONIZED_BANDS,
        include: Optional[list[str]] = None) -> pd.DataFrame:
    """ add_spectral_indices

    Creates a copy of passed dataframe with (array-value) spectral index
    columns added.

    Args:

        data (pd.DataFrame): dataframe containing (array valued) spectral band and coord columns
        name (Optional[str] str = c.DEFAULT_SPECTRAL_INDEX_CONFIG):
            NOTE: only used if `indices` below is None:
            name of, or path to, config file.
            if re.search(r'(yaml|yml)$', <name>) loads yaml file with at path at <name>
            else load yaml at '<project-root>/config/spectral_indices/<name>.yaml'
        indices (dict[str, str]): config containing spectral-index equations
        bands (list[str] = landsat.HARMONIZED_BANDS): list of spectral band nanmes used in equations
        include (Optional[list[str]] = ID_COLUMNS):
            - list of columns to keep from the original dataframe
            - if falsey all columns will be preserved

    Returns:

        (pd.DataFrame)
    """
    data = data.copy()
    if indices is None:
        assert isinstance(name, str)
        indices = index_config(name)
    index_cols = pd.Index(indices.keys())
    index_arr = data.eval(list(indices.values()))
    index_df = pd.DataFrame(index_arr.T, columns=index_cols)
    if include:
        data = data[include]
    data.loc[:, index_cols] = index_df
    return data
