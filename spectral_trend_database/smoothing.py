""" Methods for smoothing dataset

The main method, `smooth(...)`, takes a number of steps:

    a. linear interpolation
    b. remove drops
    c. sg smoothing
    d. window smoothing

License:
    BSD, see LICENSE.md
"""

import warnings
from copy import deepcopy
import numpy as np
import xarray as xr
from numpy.lib.stride_tricks import sliding_window_view
from scipy.interpolate import interp1d
import scipy.signal as sig
from datetime import timedelta
from ndvi_trends.utils.npxr import npxr, sequencer


#
# CONSTANTS
#
SAME_CONV_MODE = 'same'
VALID_CONV_MODE = 'valid'
DEFAULT_CONV_MODE = SAME_CONV_MODE
LINEAR_CONV_TYPE = 'linear'
MEAN_CONV_TYPE = 'mean'
SMOOTHING_DATA_VAR = 'ndvi'
SMOOTHING_RESULT_DATA_VARS = [
    None,
    None,
    'preprocessed_ndvi',
    'sg_ndvi'
]
COORD_VAR = 'time'
DEFAULT_SG_WINDOW_LENGTH = 60
DEFAULT_SG_POLYORDER = 3
DEFAULT_WINDOW_CONV_TYPE = MEAN_CONV_TYPE
DEFAULT_WINDOW_RADIUS = 5
MACD_DATA_VAR = 'sg_ndvi'
MACD_RESULT_DATA_VARS = ['ema_a', 'ema_b', 'macd', 'ema_c', 'macd_div']
EPS = 1e-4


#
# xr-decorated sequencer and methods
#
@npxr
def ewma(
        data,
        alpha=None,
        span=None,
        init_value='sma'):
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
        elif isinstance(a, (list, np.ndarray, xr.DataArray)):
            ewm_pre = init_value
        else:
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
def linearly_interpolate(data):
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


@npxr
def interpolate(
        data,
        kind='linear',
        extrapolate=True,
        return_data_var=False,
        data_var=None,
        result_data_var=None,
        result_prefix=None,
        result_suffix=None):
    """ interpolate series xr.dataset/data_array

    NOTE: This method is decorated by @npxr to accept/return xarray objects. See `npxr`
    doc-strings for details and description of additional args.

    Replaces np.nan in a 1-d array with interpolation

    Args:
        data (np.array): source data
        kind (str):
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
    indices = np.arange(data.shape[0])
    if extrapolate:
        fill_value = 'extrapolate'
    else:
        fill_value = None
    _interp_func = interp1d(indices[~is_nan], data[~is_nan], kind=kind, fill_value=fill_value)
    return _interp_func(indices)


@npxr
def simple_moving_average(data, win_size):
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
def kernel_smoothing(data, kernel, normalize=True):
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
    return np.convolve(data, kernel, mode=SAME_CONV_MODE)


@npxr
def mean_window_smoothing(data, radius=DEFAULT_WINDOW_RADIUS):
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
        data,
        radius,
        pad_window=1,
        pad_value=None,
        data_var=None,
        result_data_var=None):
    """ mean_window_smoothing that ignores NaNs

    Note: @npxr decorator could not be used because of multi-dim array indexing

    Args:
        data (np.array|xr.dataset/data_array): input 1-d array in which to replace data
        radius (int): half-size of window
        pad_window (int|None):
            used to preserve length of output. calculate left/rigt pad-values by taking
            the mean of the left/right-most values of length <pad_window>
        pad_value (float):
            if not <pad_window> use <pad_value> as the left/right pad-values
        data_var (str|None):
            [only used for xr.dataset] if exists update
            the named data_var. if falsey: if only 1
            data_var exists use that data_var, otherwise
            throw error.
        result_data_var (str):
            [only used for xr.dataset] name of resulting data-var.
    """
    data = data.copy()
    if isinstance(data, xr.Dataset):
        if not data_var:
            data_vars = list(data.data_vars)
            if len(data_vars) == 1:
                data_var = data_vars[0]
            else:
                err = (
                    'ndvi_trends.smoothing.nan_mean_window_smoothing: '
                    'data_var must be provided for datasets with more '
                    'that 1 data_var'
                )
                raise ValueError(err)
        arr_type = 'dataset'
        d = data[data_var].data
    elif isinstance(data, xr.DataArray):
        arr_type = 'data_array'
        d = data.data
    else:
        arr_type = 'array'
        d = data
    window = 2 * radius + 1
    pad_len = int(window / 2)
    left_pad_value, right_pad_value = _left_right_pad_values(d, pad_window, pad_value)
    d = np.concatenate([[left_pad_value] * pad_len, d, [right_pad_value] * pad_len])
    win_indices = sliding_window_view(np.arange(d.shape[0]), window_shape=window)
    win_data = d[win_indices]
    win_nan = np.isnan(win_data)
    win_mean = np.nansum(win_data, axis=1) / np.nansum(~win_nan, axis=1)
    if arr_type == 'array':
        data = win_mean
    elif arr_type == 'data_array':
        coords = list(data.coords)
        result_data_var = result_data_var or data.name
        data = xr.DataArray(win_mean, coords=data.coords).rename(result_data_var or data.name)
    else:
        coords = list(data[data_var].coords)[0]
        name = result_data_var or data[data_var].name
        data[name] = coords, win_mean
    return data


@npxr
def linear_window_smoothing(data, radius=DEFAULT_WINDOW_RADIUS, slope=1):
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


@npxr
def remove_drops(
        data,
        drop_threshold=0.5,
        smoothing_radius=16):
    """
    Replaces points in data where the value has a large dip by

    TODO: FIX THE DOC-STRINGS

    Args:
        data (np.array|xr.dataset|xr.data_array): source np.array|xr.dataset|xr.data_array
        drop_threshold (float): replace data if data/smooth_data <= <drop_ratio>
        smoothing_radius (int):
            radius used in `linear_window_smoothing`

    Returns:
        data with drops removed and replaced by nan
    """
    data = data.copy()
    # compute if dask array
    try:
        data = data.compute()
    except:
        pass
    test_data = nan_mean_window_smoothing(data, radius=smoothing_radius)
    test = (data / test_data) < drop_threshold
    data[test] = np.nan
    return data


@npxr
def replace_windows(data, replacement_data, indices, radius=1):
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
def npxr_execute(data, func, **kwargs):
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


def npxr_savitzky_golay(data, window_length=20, polyorder=3, **kwargs):
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
        data,
        spans,
        data_var=MACD_DATA_VAR,
        result_data_vars=MACD_RESULT_DATA_VARS,
        ewma_init_value='sma'):
    """ moving average convergence divergence

    Computes Moving Average Convergence Divergence (MACD). For len(<spans>) == 3,
    it also computes MACD-divergence.

    Args:
        data (xr.dataset|xr.data_array): source np.array|xr.dataset|xr.data_array
        spans (list[int]): list of window_sizes. Must have exactly 2 or 3 elements.
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
            data = {dvar: values for (dvar, values) in zip(result_data_vars, results)}
            if data_var not in result_data_vars:
                data[data_var] = da
            data = xr.Dataset(data)
    return data


def savitzky_golay_processor(
        data,
        data_var=SMOOTHING_DATA_VAR,
        result_data_vars=SMOOTHING_RESULT_DATA_VARS,
        window_length=DEFAULT_SG_WINDOW_LENGTH,
        polyorder=DEFAULT_SG_POLYORDER,
        daily_args=None,
        remove_drops_args=None,
        interpolate_args=None,
        **kwargs):
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
        interpolate,
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
        func_list=func_list,
        args_list=args_list,
        result_data_vars=result_data_vars)


#
# XARRAY
#
def daily_dataset(
        data,
        data_var=None,
        days=1,
        coord_var=COORD_VAR,
        result_data_var=None,
        start_date=None,
        end_date=None,
        method=None):
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
        coord_var (str=COORD_VAR): name of coordinate (time)
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
        da = data[data_var]
        if not result_data_var:
            result_data_var = data_var
    else:
        da = data
    coord_name = list(da.coords)[0]
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=UserWarning)
        da[coord_name] = da[coord_name].data.astype('datetime64[D]')
    da = da.groupby(coord_name, squeeze=False).mean(skipna=True)
    if not start_date:
        start_date = data[coord_name].data[0]
    if not end_date:
        end_date = data[coord_name].data[-1]
    daily_dates = np.arange(start_date, end_date, timedelta(days=days)).astype('datetime64[D]')
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=UserWarning)
        da = da.reindex({coord_var: daily_dates}, method=method)
    if result_data_var:
        return xr.Dataset(data_vars={result_data_var: da})
    else:
        return da


#
# INTERNAL
#
def _left_right_pad_values(data, window, value):
    if window:
        is_nan = np.isnan(data)
        if is_nan[:window].all():
            left_pad_value = data[~is_nan][0]
        else:
            left_pad_value = np.nanmean(data[:window])
        if is_nan[-window:].all():
            right_pad_value = data[~is_nan][-1]
        else:
            right_pad_value = np.nanmean(data[-window:])
    else:
        left_pad_value = right_pad_value = value
    return left_pad_value, right_pad_value
