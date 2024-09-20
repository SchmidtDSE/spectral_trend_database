""" Methods for smoothing dataset

The main method, `smooth(...)`, takes a number of steps:

    a. linear interpolation
    b. remove drops
    c. sg smoothing
    d. window smoothing

License:
    BSD, see LICENSE.md
"""
from typing import Callable, Union, Optional, Literal, TypeAlias, Sequence, Any
import warnings
from copy import deepcopy
import numpy as np
import xarray as xr
import dask.array
from numpy.lib.stride_tricks import sliding_window_view
from scipy.interpolate import interp1d  # type: ignore[import-untyped]
import scipy.signal as sig  # type: ignore[import-untyped]
from datetime import timedelta
from spectral_trend_database import utils
from spectral_trend_database.npxr import npxr, sequencer
from spectral_trend_database import types


#
# CONSTANTS
#
SAME_CONV_MODE: Literal['same'] = 'same'
VALID_CONV_MODE: Literal['valid'] = 'valid'
FULL_CONV_MODE: Literal['full'] = 'full'
DEFAULT_CONV_MODE: types.CONV_MODE = SAME_CONV_MODE
LINEAR_CONV_TYPE = 'linear'
MEAN_CONV_TYPE = 'mean'
SMOOTHING_DATA_VAR = 'ndvi'
SMOOTHING_RESULT_DATA_VARS = [
    None,
    None,
    'preprocessed_ndvi',
    'sg_ndvi'
]
COORD_NAME = 'date'
DEFAULT_SG_WINDOW_LENGTH = 60
DEFAULT_SG_POLYORDER = 3
DEFAULT_WINDOW_CONV_TYPE = MEAN_CONV_TYPE
DEFAULT_WINDOW_RADIUS = 5
MACD_DATA_VAR = 'sg_ndvi'
MACD_RESULT_DATA_VARS = ['ema_a', 'ema_b', 'macd', 'ema_c', 'macd_div']
EPS = 1e-4
FNN_NULL_VALUE = np.nan


#
# xr-decorated sequencer and methods
#
@npxr
def ewma(
        data: np.ndarray,
        alpha: Optional[float] = None,
        span: Optional[int] = None,
        init_value: types.EWM_INITALIZER = 'sma') -> np.ndarray:
    """ exponentially weighted moving average

    NOTE: This method is decorated by @npxr to accept/return xarray objects. See `npxr`
    doc-strings for details and description of additional args.

    Args:
        data (np.array): source np.array
        alpha (float[0,0.8]|None):
            smoothing factor: required if span is None. 1 - no smoothing, 0 - max smoothing.
            note: max alpha of 0.8 is because in our formulation we are requiring span > 1.
        span (int|None):
            effective window size: required if alpha is None (slpha = 2 / (alpha + 1))
        init_value ('sma'|'mean'|float|list/array|func|False):
            if init_value:
                use data[0] as first/0-th term (ewm_0) in exponentially weighted moving average
                sets the first/0-th term (ewm_0) and any initial values (values_in) of the
                series.

                * 'sma':
                    use simple moving avg (win-size span) of data[:span] as input
                * 'mean':
                    use the mean of the first <span> values as inpput
                * float:
                    set the input value to <init_value>
                * list/array:
                   a list of initial values (ie np.concatenate( <values_in>, [ewm_0]))
                * func:
                    a function that takes <data[:span]> and returns a list of
                    initial values (ie np.concatenate( <values_in>, [ewm_0] ))

                Note: this may change the length of the series (ie input and output data may
                have different lengths)
            else:
                use data[0] as first/0-th term (ewm_0) in exponentially weighted moving average
        return_computed (bool):
            if True computed span/alpha along with smoothed data

    Returns:
        (np.array) Exponentially weighted moving average.
    """
    data = deepcopy(data)
    if span:
        if alpha:
            err = (
                'ndvi_trends.smoothing.ewma_array: '
                'must pass <span> or <alpha> but not both'
            )
            raise ValueError(err)
        else:
            alpha = 2 / (span + 1)
    else:
        assert isinstance(alpha, float)
        span = round((2 / alpha) - 1)
    if span < 2:
        err = (
            'ndvi_trends.smoothing.ewma_array: '
            f'span [{span}] must be greater than 1'
        )
        raise ValueError(err)
    len_in = len(data)
    if init_value:
        if init_value == 'sma':
            ewm_pre = simple_moving_average(data[:span], span)
        elif init_value == 'mean':
            ewm_pre = [data[:span].mean()]
        elif isinstance(init_value, float):
            ewm_pre = [init_value]
        elif isinstance(init_value, (list, np.ndarray, xr.DataArray)):
            ewm_pre = init_value
        else:
            assert callable(init_value)
            ewm_pre = init_value(data[:span])
        ewm_0 = ewm_pre[-1]
        values_in = ewm_pre[:-1]
        data = np.concatenate([[ewm_0], data[span:]])
    else:
        values_in = None
    data[0] = data[0] / alpha
    size = data.shape[0]
    summands = data * ((1 - alpha)**(-np.arange(size)))
    data = alpha * (1 - alpha)**(np.arange(size)) * summands.cumsum()
    if values_in is not None:
        data = np.concatenate([values_in, data])
    return data


@npxr
def linearly_interpolate(data: np.ndarray) -> np.ndarray:
    """ linearly interpolate time series

    NOTE: This method is decorated by @npxr to accept/return xarray objects. See `npxr`
    doc-strings for details and description of additional args.

    Replaces np.nan in a 1-d array with linear interpolation

    Args:
        data (np.array|xr.data_array): 1-d np-array

    Returns:
        np.array with np.nan replaced by with linear interpolation
    """
    nb_points = len(data)
    indices = np.arange(nb_points)
    notna = ~np.isnan(data)
    if not isinstance(notna, np.ndarray):
        try:
            notna = notna.compute()
        except:
            pass
    return np.interp(
        indices,
        indices[notna],
        data[notna])


def interpolate_na(
        data: types.NPXR,
        coord_name: str = COORD_NAME,
        method: str = 'linear',
        extrapolate: bool = True,
        **kwargs) -> types.NPXR:
    """ convience wrapper for dataset/data_array interpolate_na and scipy interp1d

    Note: this wrapper is particualry useful for func_list in npxr.sequencer

    Args:
        data (types.NPXR): source data
        coord_name (str = COORD_NAME): (xr only) name of coordinate ,
        method (str = 'linear'):
            if np:
                one of 'linear', 'nearest', 'nearest-up', 'zero', 'slinear',
                'quadratic', 'cubic', 'previous' or 'next'. for details see
                https://docs.scipy.org/doc/scipy-1.12.0/reference/generated/
                    scipy.interpolate.interp1d.html
            if xr:
                one of 'linear','nearest','zero','slinear','quadratic','cubic',
                'polynomial','barycentric','krogh','pchip','spline','akima'
        extrapolate (bool = True):
            (np only) use "extrapolate" to allow predictions outside of bounds. note this
            will override passing 'fill_value' as a kwarg. for details see
            https://docs.scipy.org/doc/scipy-1.12.0/reference/generated/
                scipy.interpolate.interp1d.html

    """
    if isinstance(data, np.ndarray):
        return np_interpolate_na(
            data=data,
            method=method,
            extrapolate=extrapolate,
            **kwargs)
    else:
        return data.interpolate_na(
            dim=coord_name,
            method=method,
            **kwargs)


def np_interpolate_na(
        data: np.ndarray,
        method: str = 'linear',
        extrapolate: bool = True,
        **kwargs) -> np.ndarray:
    """ interpolate series np.array

    Replaces np.nan in a 1-d array with interpolation

    Args:
        data (np.array): source data
        method (str):
            one of 'linear', 'nearest', 'nearest-up', 'zero', 'slinear',
            'quadratic', 'cubic', 'previous' or 'next'. for details see
            https://docs.scipy.org/doc/scipy-1.12.0/reference/generated/
                scipy.interpolate.interp1d.html
        extrapolate (bool):
            use "extrapolate" to allow predictions outside of bounds. for details see
            https://docs.scipy.org/doc/scipy-1.12.0/reference/generated/
                scipy.interpolate.interp1d.html

    Returns:
        (np.array|xr.dataset|xr.data_array) linearly interpolated data
        if <return_data_var> return tuple (data, <result_data_var>)
    """
    data = data.copy()
    is_nan = np.isnan(data)
    indices = np.arange(utils.npxr_shape(data)[0])
    if extrapolate:
        kwargs['fill_value'] = 'extrapolate'
    _interp_func = interp1d(
        indices[~is_nan],
        data[~is_nan],
        kind=method,
        **kwargs)
    return _interp_func(indices)


@npxr
def simple_moving_average(data: np.ndarray, win_size: int) -> np.ndarray:
    """ simple moving average

    NOTE: This method is decorated by @npxr to accept/return xarray objects. See `npxr`
    doc-strings for details and description of additional args.

    Traditional simple moving average, with leading edge calculated so they are the average
    of all available data. Namely,

    for j < win_size:  avg_{j} = arr[:j].mean()

    Args:
        data (np.array): 1-d numpy array
        win_size (int): window-size

    Returns:
        simple moving average of <arr>
    """
    n = data.shape[0]
    numerators = np.convolve(data, np.ones(win_size), mode='full')[:n]
    denominators = np.clip(np.arange(1, n + 1), 1, win_size)
    return numerators / denominators


@npxr
def kernel_smoothing(data: np.ndarray, kernel: np.ndarray, normalize: bool = True) -> np.ndarray:
    """

    NOTE: This method is decorated by @npxr to accept/return xarray objects. See `npxr`
    doc-strings for details and description of additional args.

    Smoothes data by convolution over kernel

    Args:
        data (np.array|xr.dataset|xr.data_array): source np.array|xr.dataset|xr.data_array
        kernel (np.array): kernel for convolution
        normalize (bool):
            if true normalize kernel by `<kernel>=<kernel>/<kernel>.sum()`

    Returns:
        (np.array) data convolved over kernel
    """
    data = data.copy()
    if normalize:
        kernel = kernel / kernel.sum()
    return np.convolve(data, kernel, mode=DEFAULT_CONV_MODE)


@npxr
def mean_window_smoothing(data: np.ndarray, radius: int = DEFAULT_WINDOW_RADIUS) -> np.ndarray:
    """

    NOTE: This method is decorated by @npxr to accept/return xarray objects. See `npxr`
    doc-strings for details and description of additional args.

    Smoothes data by replacing values with mean over window

    Args:
        data (np.array): input 1-d array in which to replace data
        radius (int): half-size of window

    Returns:
        (np.array) mean-window-smoothed version of data
    """
    kernel = np.ones(2 * radius + 1)
    return kernel_smoothing(data, kernel)


def nan_mean_window_smoothing(
        data: types.NPXR,
        radius: int,
        pad_window: Optional[int] = 1,
        pad_value: Optional[float] = None,
        data_vars: Optional[Sequence[str]] = None,
        exclude: Optional[Sequence[str]] = None,
        rename: dict[str, str] = {}) -> types.NPXR:
    """ mean_window_smoothing that ignores NaNs

    Note: @npxr decorator could not be used because of multi-dim array indexing

    Args:
        data (types.NPXR): input 1-d array in which to replace data
        radius (int): half-size of window
        pad_window (Optional[int] = 1):
            used to preserve length of output. calculate left/rigt pad-values by taking
            the mean of the left/right-most values of length <pad_window>
        pad_value ( Optional[float]):
            if not <pad_window> use <pad_value> as the left/right pad-values
        data_vars (Optional[Sequence[str]]):
            [only used for xr.dataset] list of data_vars to include
        exclude (Optional[Sequence[str]]):
            [only used for xr.dataset] list of data_vars to exclude
        rename (dict):
            [only used for xr.dataset] mapping from data_var name to renamed data_var name
    """
    data = data.copy()
    if data_vars:
        data_vars = [v for v in data_vars if v not in (exclude or [])]
    values = utils.to_ndarray(data, data_vars=data_vars)
    window = 2 * radius + 1
    pad_length = int(window / 2)
    values = _left_right_pad(
        values,
        pad_length=pad_length,
        window=pad_window,
        value=pad_value)
    windows = sliding_window_view(values, window_shape=(values.shape[0], window))[0]
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=RuntimeWarning)
        win_mean = np.nanmean(windows, axis=-1).T
    if isinstance(data, np.ndarray):
        data = win_mean
    elif isinstance(data, (xr.DataArray, dask.array.Array)):
        data.data = win_mean
        new_name = rename.get(data.name)
        if new_name :
            data = data.rename(new_name)
    else:
        assert isinstance(data, xr.Dataset)
        data = utils.replace_dataset_values(
            data,
            values=win_mean,
            data_vars=data_vars,
            rename=rename)
    return data




@npxr
def linear_window_smoothing(
        data: np.ndarray,
        radius: int = DEFAULT_WINDOW_RADIUS,
        slope: float = 1.0) -> np.ndarray:
    """

    NOTE: This method is decorated by @npxr to accept/return xarray objects. See `npxr`
    doc-strings for details and description of additional args.

    Smoothes data by replacing values with weighted-mean over window

    Args:
        data (np.array): input 1-d array in which to replace data
        radius (int): half-size of window
        slope (float): slope of line

    Returns:
        (np.array) linear-window-smoothed version of data
    """
    left = slope * (np.arange(radius) + 1)
    right = left[::-1]
    kernel = np.concatenate([left, [left[-1] + slope], right])
    return kernel_smoothing(data, kernel)


def remove_drops(
        data: types.NPXR,
        drop_threshold: float = 0.5,
        smoothing_radius: int = 16,
        smoothing_pad_window: Optional[int] = 1,
        smoothing_pad_value: Optional[float] = None,
        rename: dict[str, str] = {}) -> types.NPXR:
    """
    Replaces points in data where the value has a large dip by

    Args:
        data (types.NPDASK): source np.array|xr.dataset|xr.data_array
        drop_threshold (float = 0.5): replace data if data/smooth_data <= <drop_ratio>
        smoothing_radius (int = 16):
            radius used in `linear_window_smoothing`
        smoothing_pad_window (Optional[int] = 1):
            used to preserve length of output. calculate left/rigt pad-values by taking
            the mean of the left/right-most values of length <pad_window>
        smoothing_pad_value ( Optional[float]):
            if not <pad_window> use <pad_value> as the left/right pad-values

    Returns:
        data with drops removed and replaced by nan
    """
    data = data.copy()
    if isinstance(data, dask.array.Array):
        data = data.compute()
    values = utils.to_ndarray(data)
    test_data = nan_mean_window_smoothing(
        values,
        radius=smoothing_radius,
        pad_window=smoothing_pad_window,
        pad_value=smoothing_pad_value)
    test = (values / test_data) < drop_threshold
    values[test] = np.nan

    if isinstance(data, np.ndarray):
        data = values
    elif isinstance(data, (xr.DataArray, dask.array.Array)):
        data.data = values
        new_name = rename.get(data.name)
        if new_name :
            data = data.rename(new_name)
    else:
        assert isinstance(data, xr.Dataset)
        data = utils.replace_dataset_values(
            data,
            values=values,
            rename=rename)
    return data



@npxr
def replace_windows(
        data: np.ndarray,
        replacement_data: np.ndarray,
        indices: Union[np.ndarray, list],
        radius: int = 1) -> np.ndarray:
    """ replace data with replacement data for windows around indices


    NOTE: This method is decorated by @npxr to accept/return xarray objects. See `npxr`
    doc-strings for details and description of additional args.

    Replaces data around indices.  For instance, if <radius>=2 and
    there is an index=6 the following data will be replaced

    `data[[4,5,6,7,8]] = replacement_data[[4,5,6,7,8]]`

    Args:
        data (np.array): input 1-d array in which to replace data
        replacement_data 1-d array to replace data with
        indices (list|np.array): indices around wich to replace data
        radius (int): half-size of window
    Returns:
        np.array with data around <indices> replaced
    """
    data = data.copy()
    indices = [range(i - radius, i + radius + 1) for i in indices]
    indices = np.array([i for r in indices for i in r])
    indices = np.unique(indices.clip(0, len(data) - 1))
    data[indices] = replacement_data[indices]
    return data


@npxr
def npxr_execute(data: np.ndarray, func: Callable, **kwargs) -> Any:
    """
    Wrapper that extends function that takes and returns np.array to return
    xarray objects using the @npxr decorator.  See `npxr`
    doc-strings for details and description of additional args.

    Args:
        data (np.array): source data
        func (function): function to exectue
            * <func> must have the first argument be of type np.array.
            * deocorated <func> must return a np.array
        **kwargs (kwargs): kwargs for <func>
    """
    return func(data, **kwargs)


def npxr_savitzky_golay(
        data: np.ndarray,
        window_length: int = 20,
        polyorder: int = 3,
        **kwargs) -> np.ndarray:
    """ wrapper for scipy's savitzky-golay filter

    NOTE: Extends function that takes and returns np.array to return
    xarray objects using the @npxr decorator.  See `npxr`
    doc-strings for details and description of additional args.

    See Scipy's documentation for details:

    https://docs.scipy.org/doc/scipy/reference/generated/scipy.signal.savgol_filter.html

    Args:
        data (np.array): data source ("x" arg for scipy.signal.savgol_filter)
        window_length (int):
            The length of the filter window (i.e., the number of coefficients). If mode is
            ‘interp’, window_length must be less than or equal to the size of x.
        polyorder (int):
            The order of the polynomial used to fit the samples. polyorder must be less than
            window_length.
        **kwargs (kwargs):
            includes deriv, delta, axis, mode, cval (see scipy docs for details)
    """
    return npxr_execute(
        data,
        func=sig.savgol_filter,
        window_length=window_length,
        polyorder=polyorder,
        **kwargs)


#
# SEQUENCES
#
def macd_processor(
        data: types.XR,
        spans: Sequence[int],
        data_var: Optional[str] = MACD_DATA_VAR,
        result_data_vars: Optional[Sequence[Union[str, None]]] = MACD_RESULT_DATA_VARS,
        ewma_init_value: types.EWM_INITALIZER = 'sma') -> types.XR:
    """ moving average convergence divergence

    Computes Moving Average Convergence Divergence (MACD). For len(<spans>) == 3,
    it also computes MACD-divergence.

    Args:
        data (xr.dataset|xr.data_array): source np.array|xr.dataset|xr.data_array
        spans (Sequence[int]): list of window_sizes. Must have exactly 2 or 3 elements.
            if len(<spans>) == 2:
                compute and return the moving-average-convergence-divergence
                `macd_values = ewma(data, spans[0]) - ewma(data, spans[1])`
            elif len(<spans>) == 3:
                compute `macd_values` as above and then compute the MACD divergence
                `macd_div = macd_values - ewma(macd_values, spans[2])`
        data_var (str): if <data> is xr.dataset, the name of the data_variable to
            compute macd values from
        result_data_vars (None|str|list):
        ewma_init_value (str): `init_value` argument for `ewma`. see ewma-doc-strings
            for details.

    Returns:
        if data is np.array: returns macd_values or macd_div_values (depending on len(<spans>))
        if data is xr.data_array:
            if result_data_vars is list: returns xr.dataset with ewm_a/b, macd[, ewm_c, macd_div]
            else: return xr.data_array for macd_values or macd_div_values
        else:
            return appended|overwritten xr.dataset
    """
    len_spans = len(spans)
    if not result_data_vars:
        result_data_vars = data_var
    is_dataset = isinstance(data, xr.Dataset)
    if is_dataset:
        da = data[data_var]
    else:
        da = data
    ewm_a = ewma(da, span=spans[0], init_value=ewma_init_value)
    ewm_b = ewma(da, span=spans[1], init_value=ewma_init_value)
    macd_values = ewm_a - ewm_b
    results = [ewm_a, ewm_b, macd_values]
    if len_spans == 3:
        ewm_c = ewma(macd_values, span=spans[2], init_value=ewma_init_value)
        macd_div_values = macd_values - ewm_c
        results.append(ewm_c)
        results.append(macd_div_values)
    if isinstance(data, np.ndarray):
        return results[-1]
    elif is_dataset:
        if isinstance(result_data_vars, str):
            data[result_data_vars] = results[-1]
        elif isinstance(result_data_vars, list):
            for dvar, values in zip(result_data_vars, results):
                data[dvar] = values
    else:
        if isinstance(result_data_vars, str):
            data = results[-1]
            data.name = result_data_vars
        elif isinstance(result_data_vars, list):
            data_value_dict = {dvar: values for (dvar, values) in zip(result_data_vars, results)}
            if data_var not in result_data_vars:
                data_value_dict[data_var] = da
            data = xr.Dataset(data_value_dict)
    return data


def savitzky_golay_processor(
        data: types.NPXR,
        data_var: Optional[str] = SMOOTHING_DATA_VAR,
        result_data_vars: Optional[Sequence[Union[str, None]]] = SMOOTHING_RESULT_DATA_VARS,
        window_length: int = DEFAULT_SG_WINDOW_LENGTH,
        polyorder: int = DEFAULT_SG_POLYORDER,
        daily_args: Optional[types.ARGS_KWARGS] = None,
        remove_drops_args: Optional[types.ARGS_KWARGS] = None,
        interpolate_args: Optional[types.ARGS_KWARGS] = None,
        **kwargs) -> types.NPXR:
    """

    !!!!!!!!!!!!!!!!! WIP !!!!!!!!!!!!!!!!!

    Wrapper for `ndvi_trends.utils.npxr.sequence` to run a series of smoothing steps

    Steps:

    1. create (n-)daily dataset by filling with np.nan for missing dates
    1. interpolate to fill np.nan
    2. remove "dips" - sudden large drops in the data that bounce back
    3. apply a smoothing func (defaults to Savitzky-Golay filter)

    Args:
        TODO:...

    Returns:
        (xr.dataset|xr.data_array) data with smoothed data values
    """
    kwargs['window_length'] = window_length
    kwargs['polyorder'] = polyorder
    func_list = [
        daily_dataset,
        remove_drops,
        interpolate_na,
        npxr_savitzky_golay
    ]
    args_list = [
        daily_args,
        remove_drops_args,
        interpolate_args,
        kwargs
    ]
    return sequencer(
        data,
        data_var=data_var,
        # mypy bug: https://github.com/python/mypy/issues/14319
        func_list=func_list,  # type: ignore[arg-type]
        args_list=args_list,
        result_data_vars=result_data_vars)


#
# XARRAY
#
def daily_dataset(
        data: types.XR,
        data_var: Optional[str] = None,
        days: int = 1,
        result_data_var: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        method: Optional[types.FILL_METHOD] = None) -> types.XR:
    """ transform a dataset to a (n-)day dataset

    takes a dataset with datetime coordinate and returns
    the same data at regular n-day (n=<days>) dataset.

    the additional days added to the series are by default
    filled with np.nan but adjust with the <method> argument.

    Args:
        data (xr.dataset|xr.data_array): data to be transformed
        data_var (str|None):
            if <data_var> is None: <data> must be an xr.data_array.
            otherwise: name of data_var. <data> must be an xr.dataset.
        days (int): number of days between points (defaults to daily)
        result_data_var (str|None):
            if <data_var> is None:
                if <result_data_var>: return data as xr.dataset with
                    data_var named <result_data_var>
                otherwise: return data as xr.data_array
            else:
                data will be returned as xr.dataset. if <result_data_var>
                is None, <data_var> will be used as data_var name.
        start/end_date (str['%y-%M-%d']|None):
            start/end_date. if None use first and/or last date in <data>
        method (str):
            one of [None, 'nearest', 'pad'/'ffill', 'backfill'/'bfill'] (for details:
            https://docs.xarray.dev/en/stable/generated/xarray.DataArray.reindex.html).

    Returns:
        xr.dataset or xr.data_arrray with regualry spaced <n-day> series.
    """
    data = data.copy()
    if data_var:
        data = data[data_var]
        if not result_data_var:
            result_data_var = data_var
    coord_name = utils.xr_coord_name(data)
    if not start_date:
        start_date = data[coord_name].data[0]
    if not end_date:
        end_date = data[coord_name].data[-1]
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=UserWarning)
        data[coord_name] = data[coord_name].data.astype('datetime64[D]')
    data = data.groupby(coord_name, squeeze=False).mean(skipna=True)

    daily_dates = np.arange(start_date, end_date, timedelta(days=days)).astype('datetime64[D]')
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=UserWarning)
        data = data.reindex({coord_name: daily_dates}, method=method)
    if result_data_var:
        return xr.Dataset(data_vars={result_data_var: data})
    else:
        return data


#
# INTERNAL
#
def _first_non_nan_1d(arr: np.ndarray) -> float:
    """ returns first non nan value for 1d array

    To be used in congunction with np.apply_along_axis to get first
    non nan along axis
    """
    values = arr[~np.isnan(arr)]
    try:
        return values[0]
    except IndexError as e:
        return FNN_NULL_VALUE


def _left_right_pad(
        data: np.ndarray,
        pad_length: int = 0,
        window: Optional[int] = 1,
        value: Optional[float] = -1) -> np.ndarray:
    """ symmetrically pad 1 or 2-d array

    Note: if data is of shape (N, M) the returned array will be
    of shape (N + <pad_length>, M + <pad_length>).

    Args:
        data (np.ndarray): 1 or 2-d array to pad
        pad_length (int = 0): number of padded values to add (per-side)
        window (Optional[int] = 1):
            if None use <value> for both left/right pad values
            otherwise compute left/right means of the edge <pad_length> values to
            compute the left/right pad values
        value (Optional[float] = -1):
            if window is None: use <value> for both left/right pad values

    Returns:
        (np.ndarray) padded array
    """
    if pad_length:
        ndim = data.ndim
        if ndim == 1:
            data = np.exapnd_dims(data, axis=1)
        if window:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", category=RuntimeWarning)
                lmean = np.nanmean(data[:,:window], axis=1)
                rmean = np.nanmean(data[:,-window:], axis=1)
            lfnn = np.apply_along_axis(_first_non_nan_1d, axis=1, arr=data)
            rfnn = np.apply_along_axis(_first_non_nan_1d, axis=1, arr=data[:, ::-1])
            lpad = np.where(np.isnan(lmean), lfnn, lmean)
            rpad = np.where(np.isnan(rmean), rfnn, rmean)
            lpad = np.expand_dims(lpad, axis=1)
            rpad = np.expand_dims(rpad, axis=1)
        else:
            lpad = rpad = np.full((ndim, 1), value)
        data = np.hstack(([lpad] * pad_length) + [data] + ([rpad] * pad_length))
        if ndim == 1:
            data = data[:, 0]
    return data
