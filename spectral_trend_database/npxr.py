from typing import Any, Callable, Union, Optional, Literal, Sequence, Literal
from copy import deepcopy
from functools import wraps
import numpy as np
import xarray as xr
import dask.array
from spectral_trend_database import types
from spectral_trend_database import utils


#
# CONSTANTS
#
NP_ARRAY_TYPE = 'array'
DATA_ARRAY_TYPE = 'data_array'
DATASET_TYPE = 'dataset'
REINDEX_DROP_INIT = 'drop_init'
REINDEX_DROP_LAST = 'drop_last'


#
# DECORATORS
#
def npxr(along_axis: Union[int, Literal[False]] = False) -> Callable:
    """ npxr

    decorator for functions that take in and return
    numpy arrays to extend their behavior to xarray.

    Note:

    decorated functions must take data (types.NPXR)
    as the initial argument plus additional variable
    length args and kwargs.

    Usage:

    ```python
    @npxr()
    def plus1(arr):
        return arr + 1

    ds_plus_1 = plus1(ds)               # returns xr.Dataset
    da_plus_1 = plus1(ds.blah)          # returns xr.DataArray
    np_plus_1 = plus1(ds.blah.data)     # returns np.data
    ```

    Now let `values = utils.to_ndarray(ds)` be the array
    representation of `ds.data_vars`.

    ```python
    @npxr(along_axis=1)
    def plus1_along_axis(arr):
        return arr + 1

    p1 = plus1(values)
    p1_along_axis = plus1_along_axis(values)
    ```

    Here `p1` is equal to `values + 1` and
    `p1_along_axis` is equal to

    ```python
    np.apply_along_axis(
        lambda a: a + 1,
        axis=1,
        arr=values)
    ```

    Note: this is the same result as

    ```python
    p1_along_axis_v2 = plus1(values, along_axis=1)
    ```

    (see **kwargs below).

    Decorator Args:

        along_axis (Union[int, Literal[False]] = False):
            if (int): use np.apply_along_axis to apply
            the decorated function along axis=<along_axis>
            otherwise: apply decorator on the full data

    Args:

        data_vars (Optional[Sequence[str]] = None):
            (xr.dataset only) list of data_var names to include. if None all data_vars will be used
        exclude (Sequence[str] = []):
            (xr.dataset only) list of data_var names to exclude.
        rename (dict):
            [only used for xr data] mapping from data_var name to renamed data_var name
        **kwargs:
            additional kwargs to be passed to decorated function.
            - if <kwargs> contains 'data', that will be popped from kwargs.
            and used as the source data
            - if <kwargs> contains 'along_axis', that will be popped out and
            used to override the decorator-arg (along_axis)

    Returns:

        decorated function that accepts xr.dataset/data_array as well as np.ndarray
    """
    def _wrapper(func: Callable):
        @wraps(func)
        def _func(
                *args,
                data_vars: Optional[Sequence[str]] = None,
                exclude: Sequence[str] = [],
                rename: dict[str, str] = {},
                along_axis_override: Optional[Union[int, Literal[False]]] = None,
                **kwargs) -> types.NPXR:
            _along_axis = kwargs.pop('along_axis', along_axis)
            return execute_func(
                *args,
                func=func,
                along_axis=_along_axis,
                data_vars=data_vars,
                exclude=exclude,
                rename=rename,
                **kwargs)
        return _func
    return _wrapper


#
# METHODS
#
def sequencer(
        data: types.NPXR,
        data_vars: Optional[Sequence[Union[str, Sequence]]] = None,
        exclude: Optional[Sequence[Union[str, Sequence]]] = None,
        rename: Union[dict[str, str], Sequence[dict[str, str]]] = {},
        func_list: Sequence[Callable] = [],
        args_list: Sequence[types.ARGS_KWARGS] = []) -> types.NPXR:
    """ run a sequence of npxr-decorated methods

    Args:

        data (types.NPXR): source data
        data_vars (Optional[Sequence[Union[str, Sequence]]] = None):
            (xr.dataset only) list, or list of lists, of data_var names to include.
            if None all data_vars will be used. if list of "lists the list" must be
            the same length as <func_list>
        exclude (Optional[Sequence[Union[str, Sequence]]] = None):
            (xr.dataset only) list, or list of lists, of data_var names to exclude.
            if "list of lists" the list must be the same length as <func_list>
        rename (Union[dict[str, str], Sequence[dict[str, str]]]):
            [only used for xr data] mapping, or list of mappings, from data_var name
            to renamed data_var name. if "list of mappings" the list must be the same
            length as <func_list>
        func_list (Sequence[Callable]):
            ordered list of functions to execute
        args_list (Sequence[Union[list, dict, Literal[False]]]):
            list of arguments for aligned function. an element "<args>" should be
                - False: to skip this function
                - a tuple such that, `args, kwargs = <args>` and func(data, *args, **kwargs)
                - a list such that, `args = <args>` and func(data, *args)
                - a dict such that, `kwargs = <args>` and func(data, **kwargs)
                - otherswise, such that, func(data, <args>)

    Returns:

        output data (and possibly intermediate steps) after processing through sequence.
        form will be the same as type as the input data (np.array|xr.data_array|xr.dataset)
    """
    nb_funcs = len(func_list)
    data_vars = _lists_of(nb_funcs, data_vars)
    exclude = _lists_of(nb_funcs, exclude)
    rename = _lists_of(nb_funcs, rename)
    args_zip = zip(func_list, args_list, data_vars, exclude, rename)
    for i, (func, args, _d, _e, _r) in enumerate(args_zip):
        if args is not False:
            args, kwargs = _process_sequence_function_args(args)
            data = func(
                data,
                *args,
                data_vars=_d,
                exclude=_e,
                rename=_r,
                **kwargs)
    return data


def execute_func(
        *args,
        func: Callable,
        data: Optional[types.NPDXR] = None,
        along_axis: Union[int, Literal[False]] = False,
        data_vars: Optional[Sequence[str]] = None,
        exclude: Optional[Sequence[str]] = None,
        rename: dict[str, str] = {},
        **kwargs) -> types.NPXR:
    """
    Note:

    the source data is not passed as a key-word argument
    it is assumed that the first (variable length) argument
    is <data>. Namely `data = args[0]`

    Args:

        func (Callable):
            function to be called on data. <func> must take
            data (types.NPXR) as the initial argument plus
            additional variable length args and kwargs.
        along_axis (Union[int, Literal[False]] = False):
            if (int): use np.apply_along_axis to apply
            the decorated function along axis=<along_axis>
            otherwise: apply decorator on the full data
        data_vars (Optional[Sequence[str]] = None):
            (xr.dataset only) list of data_var names to include.
            if <data_vars> is None all data_vars will be used
        exclude (Optional[Sequence[str]] = None):
            (xr.dataset only) list of data_var names to exclude.
        rename (dict):
            [only used for xr data] mapping from data_var name to renamed data_var name
        *args:
            additional variable length arguments to be passed to <func>
            if <data> is None: data = <args>[0] and <args>[1:]
            becomes the additional variable length arguments

        **kwargs:
            additional kwargs to be passed to <func>. if <kwargs> contains
            'data', that will be popped from kwargs.

    Returns:

        data processed by <func>, possibly along an axis, in the same format
        as the initial source data.
    """
    if data is None:
        data = args[0]
        args = args[1:]
    data = deepcopy(data)
    if isinstance(data, dask.array.Array):
        data = data.compute()
    assert not isinstance(data, type(None))
    values = utils.to_ndarray(
        data=data,
        data_vars=data_vars,
        exclude=exclude)
    if along_axis is False:
        values = func(values, *args, **kwargs)
    else:
        values = np.apply_along_axis(  # type: ignore[call-overload]
            func,
            *args,
            axis=along_axis,
            arr=values,
            **kwargs)
    return post_process_npxr_data(
        data=data,
        values=values,
        rename=rename)


def post_process_npxr_data(data: types.NPDXR, values: types.NPD, rename: dict[str, str] = {}):
    """ post process npxr data
    Args:

        data (types.NPXR): source np.array|xr.dataset|xr.data_array
        values (types.NPD): processed numpy array
        rename (dict):
            [only used for xr data] mapping from data_var name to renamed data_var name
    Returns:

        data with drops removed and replaced by nan
    """
    if isinstance(data, (np.ndarray, dask.array.Array)):
        data = values
    elif isinstance(data, xr.DataArray):
        data.data = values
    else:
        assert isinstance(data, xr.Dataset)
        data = utils.replace_dataset_values(
            dataset=data,
            values=values)
    if rename:
        data = utils.npxr_rename(
            data,
            rename=rename)
    return data


#
# INTERNAL
#
def _lists_of(length: int, values: Any) -> list:
    """ lists of (length or object)

    if <values> is a tuple or list: validates `len(values) == length`
    else: converts <values> object to `[values] * length`

    Args:

        length (int): target length of returned list
        values (Any): list for length validation or object for expansion

    Returns:

        list of length <length>
    """
    if isinstance(values, (tuple, list)):
        values_length = len(values)
        if values_length:
            if length != values_length:
                err = (
                    'spectral_trend_database.npxr._lists_of: '
                    f'if values is a sequence the length ({values_length}) must be '
                    f'same as key-list ({length}).'
                )
                raise ValueError(err)
            else:
                values = list(values)
        else:
            values = [[]] * length
    else:
        values = [values] * length
    return values


def _process_sequence_function_args(
        args: Union[tuple[list, dict], Sequence, dict, None]) -> tuple[list, dict]:
    """ process arguments for functions in sequencer `func_list`

    converts element of `args_list` to args-kwargs pair for function

    Args:

        args (tuple|list|dict|object|`falsey`): element to convert

    Returns:

        (tuple) args, kwargs
    """
    if isinstance(args, tuple):
        args, kwargs = args
    elif isinstance(args, list):
        args, kwargs = args, {}
    elif isinstance(args, dict):
        args, kwargs = [], dict(args)
    elif args:
        args, kwargs = [args], {}
    elif args is None:
        args, kwargs = [], {}
    assert isinstance(args, list) and isinstance(kwargs, dict)
    return args, kwargs
