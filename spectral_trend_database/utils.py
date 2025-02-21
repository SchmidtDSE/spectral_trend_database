""" utility methods

License:
    BSD, see LICENSE.md
"""
from typing import Any, Union, Optional, Callable, Iterable, Sequence, Literal
import re
from pathlib import Path
from datetime import datetime
from copy import deepcopy
from zipfile import ZipFile
import json
import pandas as pd
import numpy as np
import xarray as xr
import requests
import dask.array
from scipy import stats  # type: ignore[import-untyped]
import yaml
from spectral_trend_database import constants
from spectral_trend_database import types


#
# CONSTANTS
#
DEFAULT_ACTION: Literal['prefix', 'suffix', 'replace'] = 'replace'
LIST_LIKE_TYPES: tuple = (list, tuple, np.ndarray, xr.DataArray, pd.Series)
DATE_FMT: str = '%Y-%m-%d'


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


def make_parent_directories(*paths):
    for p in paths:
        Path(p).parent.mkdir(parents=True, exist_ok=True)


def append_ldjson(
        dest: str,
        data: Union[list, dict],
        multiline: bool = False,
        dry_run: bool = False,
        **shared):
    """ append/create line to line deliminated json file """
    if dry_run:
        print('- dry_run [local]:', dest)
    else:
        with open(dest, "a") as file:
            if not multiline:
                data = [data]
            if shared:
                data = [{**shared, **d} for d in data]
            data = '\n'.join([json.dumps(d) for d in data])
            file.write(data + '\n')


def dataframe_to_ldjson(
        df: pd.DataFrame,
        dest: str,
        date_column: Optional[str] = 'date',
        dry_run: bool = False,
        create_dirs: bool = True,
        noisy: bool = True,
        mode: Literal['w', 'a'] = 'w') -> Union[str, None]:
    """ save dataframe locally as line-deliminated JSON

    Args:

        df (pd.DataFrame): source dataframe
        dest (str): local dest
        date_column (Optional[str] = 'date'): name of date column - attempt to convert to DATE_FMT
        dry_run (bool = True): if true print message but don't save
        create_dirs (bool = True): if true create local parent dirs if needed

    Returns:

        if not dry_run return destination otherwise None
    """
    if date_column:
        try:
            df[date_column] = df.date.apply(lambda d: d.strftime(DATE_FMT))
        except:
            pass
    if dry_run:
        if noisy:
            print('- dry_run [local]:', dest)
        dest = None
    else:
        if noisy:
            print('- local:', dest)
        if create_dirs:
            Path(dest).parent.mkdir(parents=True, exist_ok=True)
        df.to_json(dest, orient='records', lines=True, mode=mode)
    return dest


def download_and_extract_zip(
        url: str,
        path: Optional[str] = None,
        root_folder: Optional[str] = None,
        overwrite: bool = False,
        remove_zip: bool = True) -> None:
    """ dowloand and extract zip files

    Args:
        url (str): source url
        path (Optional[str] = None): dest-path of zip file. if None extract from url
        root_folder (Optional[str] = None): folder to prepend <path>
        overwrite (bool = False): if True overwrite existing data. Otherwise silently end
        remove_zip (bool = True): if True delete zip file after extraction

    Returns:
        None
    """
    if path is None:
        path = Path(url).name
    if root_folder:
        path = f'{root_folder}/{path}'
    dest_dir = re.sub(r'\.zip$', '', path)
    if overwrite or (not Path(dest_dir).is_dir()):
        Path(dest_dir).mkdir(parents=True, exist_ok=True)
        msg = f'downloading from src {url}'
        message(msg, 'utils', 'download_and_extract_zip')
        resp = requests.get(url)
        if resp.status_code == 200:
            with open(path, "wb") as f:
                f.write(resp.content)
        else:
            err = (
                f'ERROR download_and_extract_zip: '
                f'failed to download file [status-code: {resp.status_code}]')
            raise IOError(err)
        with ZipFile(path, 'r') as zip:
            msg = f'download_and_extract_zip: extracting file to "{dest_dir}"'
            message(msg, 'utils', 'download_and_extract_zip')
            zip.extractall(dest_dir)
        if remove_zip:
            Path(path).unlink()


class Timer(object):
    """ Timer: as super simple python timer

    Usage:
        timer=Timer()
        print('Timer starting at:',timer.start())
        print('start-time as timestamp:',timer.timestamp())
        ...
        print('current duration:',timer.state())
        ...
        print('Timer stops at:',timer.stop())
        print('Duration that timer ran:',timer.delta())

    """
    TIME_FORMAT='[%Y.%m.%d] %H:%M:%S'
    TIME_STAMP_FORMAT='%Y%m%d-%H%M%S'
    def __init__(self,fmt=TIME_FORMAT,ts_fmt=TIME_STAMP_FORMAT):
        self.fmt=fmt
        self.ts_fmt=ts_fmt
        self.start_datetime=None
        self.end_datetime=None
    def start(self):
        if not self.start_datetime:
            self.start_datetime=datetime.now()
            return self.start_datetime.strftime(self.fmt)
    def timestamp(self):
        if self.start_datetime:
            return self.start_datetime.strftime(self.ts_fmt)
    def state(self):
        if self.start_datetime:
            return str(datetime.now()-self.start_datetime)
    def stop(self):
        if not self.end_datetime:
            self.end_datetime=datetime.now()
        return self.end_datetime.strftime(self.fmt)
    def delta(self):
        return str(self.end_datetime-self.start_datetime)
    def now(self,fmt='time'):
        if fmt in ['t','time']:
            fmt=self.fmt
        elif fmt in ['ts','timestamp']:
            fmt=self.ts_fmt
        return datetime.now().strftime(fmt)


#
# CORE
#
def list_prefixes(src: list[str], prefixes: list[str]):
    """ prefix list with lists """
    return [f'{p}_{s}' for s in src for p in prefixes]


#
# PD/XR/NP
#
def array_row_to_xr(
        row: Union[pd.Series, dict],
        coord: str,
        data_vars: Optional[list[str]] = None,
        coord_type: Optional[str] = constants.DATETIME_NS,
        attrs: Union[bool, list[str], dict[str, Any]] = True) -> xr.Dataset:
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
        attrs (Union[bool, list[str], dict[str, Any]] = True):
            TODO ...list of keys to  ... from attributes.

    Returns:

        xr.Dataset
    """
    if data_vars is None:
        data_vars = list(row.keys())
    coord_array = row[coord]
    if coord_type:
        coord_array = np.array(coord_array).astype(coord_type)
    vars_values = [(v, row[v]) for v in data_vars if v != coord]
    alignable = [_alignable(coord_array, d) for v, d in vars_values]
    if not isinstance(attrs, dict):
        if isinstance(attrs, list):
            attrs = {k: row[k] for k in attrs}
        elif attrs:
            attrs = {k: v for (k, v), a in zip(vars_values, alignable) if not a}
        else:
            attrs = {}
    data_var_dict = {k: ([coord], v) for (k, v), a in zip(vars_values, alignable) if a}
    return xr.Dataset(data_vars=data_var_dict, coords={coord: (coord, coord_array)}, attrs=attrs)


def array_rows_to_xr(
        rows: pd.DataFrame,
        coord: str,
        attr_cols: list = [],
        list_attrs: list = [],
        sel: Optional[Union[Callable, dict]] = None,
        **sel_kwargs: dict[str, Any]) -> xr.Dataset:
    """
    Generate a xr.dataset from dataframe

    Args:

        rows (pd.DataFrame): data to convert to xarray dataset
        coord (str): coordinate row
        attr_cols (list = []): columns to use for attrs with shared values across rows
        list_attrs (list = []): columns to use for attrs that become list of row values
        sel (Optional[Union[Callable, dict]] = None):
            dict or method that takes the row (**and sel_kwargs) and returns a dict
            to be used as `ds.sel` kwargs.
        **sel_kwargs (dict[str, Any]):
            if <sel> above is callable, <sel> is called with
            the row along with these kwargs: ie `sel(row, **sel_kwargs)`

    Returns:

        rows data as xr.Dataset
    """
    if attr_cols:
        attrs = rows.reset_index(drop=True).loc[0, attr_cols].to_dict()
    else:
        attrs = {}
    for a in list_attrs:
        attrs[a] = list(rows[a].values)
    datasets = []
    for _, row in rows.iterrows():
        ds = row_to_xr(row, coord=coord)
        if sel:
            if callable(sel):
                _kwargs = sel(row, **sel_kwargs)
            else:
                _kwargs = sel
            ds = ds.sel(**_kwargs)
        ds.attrs = {}
        datasets.append(ds)
    ds = xr.concat(datasets, dim=coord)
    if attrs:
        ds.attrs = attrs
    return ds


def rows_to_xr(
        rows: pd.DataFrame,
        coord: Optional[str] = 'date',
        cols: list = [],
        attr_cols: list = [],
        list_cols: list = [],
        list_distinct_cols: list = [],
        datetime_index: bool = True) -> xr.Dataset:
    """
    Generate a xr.dataset from dataframe

    Args:

        rows (pd.DataFrame): data to convert to xarray dataset
        coord (str): coordinate row
        cols (list = []):
            list of columns to use for data_vars. if empty all columns not in
            list_cols or attr_cols will be used.
        attr_cols (list = []): columns to use for attrs with shared values across rows
        list_cols (list = []): columns to use for attrs that become list of row values
        list_distinct_cols (list = []):
            if not empty: drop-duplicates on these columns to get list_cols values
        datetime_index (bool = True): if true convert index to datetime-type

    Returns:

        rows data as xr.Dataset
    """
    if not cols:
        cols = [n for n in rows.columns if n not in attr_cols + list_cols]
    if coord not in cols:
        cols = [coord] + cols
    ds = rows[cols].set_index(coord).to_xarray()
    attrs = rows[attr_cols].iloc[0].to_dict()
    if list_cols:
        if list_distinct_cols:
            rows = rows.drop_duplicates(list_distinct_cols)
        for a in list_cols:
            attrs[a] = list(rows[a].values)
    ds.attrs = attrs
    if datetime_index:
        ds[coord] = pd.to_datetime(ds[coord])
    return ds


def xr_to_row(
        dataset: xr.Dataset,
        data_vars: Optional[Sequence[str]] = None,
        exclude: Sequence[str] = [],
        as_pandas: bool = False) -> Union[dict, pd.Series]:
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
        data[coord] = coords[coord].data
    if data_vars is None:
        data_vars = list(dataset.data_vars)
    data_vars = [d for d in data_vars if d not in exclude]
    for var in data_vars:
        data[var] = dataset.data_vars[var].data
    if as_pandas:
        data = pd.Series(data)
    return data


def xr_to_dict(
        ds: xr.Dataset,
        coord: str = 'date',
        as_list: bool = True,
        keep_attrs: bool = True) -> Union[list, dict]:
    """ dictionary from xr.dataset

    Args:
        ds (xr.Dataset): data
        coord (str = 'date'): coord-name
        as_list (bool = True):
            if true return list of dicts (equivalent to pandas 'records')
            else return dict of list/array values
        keep_attrs (bool = True):
            if true add <ds.attrs> to returned data

    Returns:
        dataset values and attributes as dict of lists or
        list of dicts
    """
    coords = ds[coord].values
    data_var_names = list(ds.data_vars)
    attrs = ds.attrs
    values = [coords] + [ds[n].data for n in data_var_names]
    if as_list:
        data = [
            {k: v for k, v in zip(data_var_names, a)}
            for a in np.array(values).T]
        if keep_attrs and attrs:
            data = [{**attrs, **d} for d in data]
    else:
        data = {k: v for k, v in zip(data_var_names, values)}
        if keep_attrs and attrs:
            data = {**attrs, **data}
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


def xr_stats(
        dataset: xr.Dataset,
        data_vars: Optional[Sequence[str]] = None,
        axis: Optional[int] = -1,
        skew_kurtosis: bool = True) -> xr.Dataset:
    """ compute stats for dataset

        Args:

            dataset (xr.Dataset): dataset on which to compute stats
            data_vars (Optional[Sequence[str]] = None):
                data_vars to compute stats on. if none use all data_vars
            axis (Optional[int] = -1): axis along which to compute stats
            skew_kurtosis (bool = True): if true also include skew, kurtosis in stats

        Returns:

            (xr.Dataset) dataset whose data_vars are stats values
    """
    if data_vars is None:
        data_vars = list(dataset.data_vars)
    arr = dataset_to_ndarray(dataset)
    stat_values = [
        arr.mean(axis=axis),
        np.median(arr, axis=axis),
        arr.min(axis=axis),
        arr.max(axis=axis)
    ]
    dvar_names = [
        [f'{n}_mean' for n in data_vars],
        [f'{n}_median' for n in data_vars],
        [f'{n}_min' for n in data_vars],
        [f'{n}_max' for n in data_vars]
    ]
    dvar_names = [
        _suffix_list(data_vars, 'mean'),
        _suffix_list(data_vars, 'median'),
        _suffix_list(data_vars, 'min'),
        _suffix_list(data_vars, 'max')]
    if skew_kurtosis:
        stat_values += [stats.skew(arr, axis=axis), stats.kurtosis(arr, axis=axis)]
        dvar_names += [_suffix_list(data_vars, 'skew'), _suffix_list(data_vars, 'kurtosis')]
    datasets = []
    for names, values in zip(dvar_names, stat_values):
        datasets.append(
            xr.Dataset(data_vars={
                n: ([], v)
                for (n, v) in zip(names, values)}))
    return xr.combine_by_coords(datasets, join='exact')  # type: ignore[return-value]


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
        dataset: xr.Dataset,
        values: types.NPD,
        data_vars: Optional[Sequence[str]] = None,
        suffix: bool = False) -> xr.Dataset:
    """ updates the values of a dataset
    """
    if data_vars is None:
        data_vars = list(dataset.data_vars)
    assert isinstance(data_vars, list)
    for dv, v in zip(data_vars, values):
        dataset[dv].data = v
    return dataset


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


def npxr_stack(
        data: Union[list[np.ndarray], list[xr.Dataset], list[xr.DataArray]],
        dim: Optional[str] = None,
        raise_align_error: bool = False) -> Union[np.ndarray, xr.Dataset, None]:
    """ safely stack datasets, data-arrays, or np.arrays

    if data is np.ndarray wrapper for np.vstack
    elif data is xr.DataArray  wrapper for stack_data_arrays above
    elif data is xr.Dataset wrapper for stack_datasets above

    Args:

        data ( Union[list[np.ndarray], list[xr.Dataset], list[xr.DataArray]]):
            list of data to stack
        dim (Optional[str] = None): [xr.dataset only] dim to stack along. if None use coord of first
            dataset in datasets
        raise_align_error (bool = False): [xr.dataset only] if True raise error if datasets
            do not align. Otherwise silently return None

    Returns:

        dataset stacking datasets along <dim>
    """
    dtype = type(data[0])
    if dtype is np.ndarray:
        stack = np.vstack(data)  # type: ignore[arg-type]
    elif (dtype is xr.DataArray) or (dtype is xr.Dataset):
        stack = xr.combine_by_coords(data, join='exact')  # type: ignore[assignment, arg-type]
    else:
        stack = None
        err = (
            'spectral_trend_database.utils.npxr_stack: '
            f'element data-types ({dtype}) must be one of '
            f'np.ndarray, xr.DataArray or xr.Dataset'
        )
        raise ValueError(err)
    return stack


def rename_data_array(
        data_array: xr.DataArray,
        rename: Optional[str] = None,
        action: Union[Literal['prefix', 'suffix', 'replace']] = DEFAULT_ACTION) -> xr.DataArray:
    """ rename data array by prefixing suffixing or replacing value
    Args:

        data_array (xr.DataArray): data-array to be renamed
        rename (Optional[str] = None): prefix, suffix or replacement value for name
        action (Union[Literal['prefix', 'suffix', 'replace']] = DEFAULT_ACTION):
            if prefix name => <rename>_<data_array>.name
            elif suffix name => <data_array>.name_<rename>
            elif replace name => <rename>
    Returns:

        renamed data-array
    """
    if rename:
        data_array.name = _name_value(str(data_array.name), rename, action)
    return data_array


def rename_dataset(
        dataset: xr.Dataset,
        rename: Optional[Union[dict[str, str], list[str], str]] = None,
        action: Union[Literal['prefix', 'suffix', 'replace']] = DEFAULT_ACTION) -> xr.Dataset:
    """ rename dataset data_vars by prefixing suffixing or replacing value
    Args:

        data_array (xr.DataArray): data-array to be renamed
        rename (Optional[Union[dict[str, str], list[str], str]] = None):
            if str: rename with <rename> for all data_vars
            elif list: rename with <rename> values for paired data_var
            elif dict: rename using dataset.rename(<rename>)
        action (Union[Literal['prefix', 'suffix', 'replace']] = DEFAULT_ACTION):
            if prefix name => <rename>_<data_array>.name
            elif suffix name => <data_array>.name_<rename>
            elif replace name => <rename>
    Returns:

        renamed data-array
    """
    if rename:
        data_vars = list(dataset.data_vars)
        if isinstance(rename, str):
            rename = [rename] * len(data_vars)
        if isinstance(rename, list):
            rename = {k: _name_value(k, v, action) for (k, v) in zip(data_vars, rename)}
        else:
            rename = {k: _name_value(k, v, action) for (k, v) in rename.items() if k in data_vars}
        dataset = dataset.rename(rename)
    return dataset


def npxr_rename(
        data: types.NPDXR,
        rename: Optional[Union[dict[str, str], list[str], str]] = None,
        action: Union[Literal['prefix', 'suffix', 'replace']] = DEFAULT_ACTION):
    """ convince wrapper for rename_dataset/data_array

    Note: if data is np.ndarray this method simply returns the passed data

    Args:

        data (types.DNPXR): np.ndarray (to be passed through) or, data-array/dataset to be renamed
        rename (Optional[Union[dict[str, str], list[str], str]] = None):
            if x.Dataset:
                if str: rename with <rename> for all data_vars
                elif list: rename with <rename> values for paired data_var
                elif dict: rename using dataset.rename(<rename>)
            elif xr.DataArray:
                prefix, suffix or replacement value for name
        action (Union[Literal['prefix', 'suffix', 'replace']] = DEFAULT_ACTION):
            if prefix name => <rename>_<data_array>.name
            elif suffix name => <data_array>.name_<rename>
            elif replace name => <rename>
    Returns:

        renamed data-array/dataset or original np.ndarray
    """
    if isinstance(data, xr.Dataset):
        data = rename_dataset(
            dataset=data,
            rename=rename,
            action=action)
    elif isinstance(data, xr.DataArray):
        assert isinstance(rename, str) or (rename is None)
        data = rename_data_array(
            data_array=data,
            rename=rename,
            action=action)
    return data


def nan_to_safe_nan(values: Union[list, np.ndarray]) -> Union[Sequence[float], float]:
    """ replace nan values with a "safe" bigquery value

    BigQuery was not working with mixed none/float array valued columns.
    `nan_to_safe_nan` and `safe_nan_to_nan` allow one to transfer back
    and forth between np.nan and constants.SAFE_NAN_VALUE

    """
    values = np.array(values).astype(float)
    values[np.isnan(values)] = constants.SAFE_NAN_VALUE
    if values.ndim:
        return list(values)
    else:
        return float(values)


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


#
# INTERNAL
#
def _name_value(
        name: str,
        value: str,
        action: Union[Literal['prefix', 'suffix', 'replace']] = 'prefix',
        sep: str = '_') -> str:
    if action == 'suffix':
        value = f'{name}{sep}{value}'
    elif action == 'prefix':
        value = f'{value}{sep}{name}'
    return value


def _suffix_list(values: Sequence[str], suffix: str, sep: str = '_') -> list[str]:
    return [f'{v}{sep}{suffix}' for v in values]


def _alignable(ref_list: list, value: Any) -> bool:
    if isinstance(value, LIST_LIKE_TYPES):
        return len(ref_list) == len(value)
    else:
        return False
