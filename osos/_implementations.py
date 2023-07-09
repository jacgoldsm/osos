import pandas as pd
import numpy as np
from pandas.core.groupby.generic import SeriesGroupBy
from typing import Union, Callable, Iterable, Mapping, Any
import itertools

from window import EmptyWindow,MAX_WINDOW_SIZE
from indexer import SparkIndexer
SeriesType = Union[pd.Series,SeriesGroupBy]

def get_rollspec(series: pd.Series,bottom: int, top: int,row_or_range='row',range_col=None) -> SparkIndexer:
    indexer = SparkIndexer(series,bottom,top,row_or_range,range_col).get_window_bounds()
    return indexer

def _get_rolling_window(series, *args, **kwargs):
    win = kwargs.pop("_over")
    df_len = series.size
    if isinstance(win,EmptyWindow):
        return series.rolling(window=df_len,center=True,min_periods=1).sum(*args, **kwargs).reset_index().astype(series.dtype)
    roll = series.copy()
    if win.partition_by is not None:
        roll = series.groupby(win.partition_by).rolling(window=df_len,center=True,min_periods=1)
    if win.order_by is not None:
        indices = np.argsort(win.order_by)
        roll = roll.sort_values(indices) # this is wrong
    # if win.rows_between is not None:
    #     assert win.order_by is not None, "rowsBetween requires an order specification"
    #     bottom,top = win.rows_between
    #     rollspec = get_rollspec(series,bottom,top)
    #     roll = roll.rolling(rollspec)
    # elif win.range_between is not None:
    #     assert win.order_by is not None, "rangeBetween requires an order specification"
    #     bottom,top = win.range_between
    #     rollspec = get_rollspec(
    #         series,
    #         bottom,
    #         top,
    #         row_or_range='row',
    #         range_col=win.order_by[0]
    #         )
    #     roll = roll.rolling(rollspec)
    return roll

def sum_func(series: SeriesType, *args, **kwargs) -> pd.Series:
    _meth = kwargs.pop("_meth")
    if _meth == 'aggregate':
        assert isinstance(kwargs.pop("_over"), EmptyWindow), "Cannot mix Window functions and Aggregate functions"
        return series.sum(*args,**kwargs).reset_index()
    roll = _get_rolling_window(series, *args, **kwargs)
    return roll.sum(*args, **kwargs).reset_index()[series.name].astype(series.dtype)



def udf_func(series: pd.Series, udf: Callable, args_and_kwargs: dict[Iterable, dict[str,Any]], _over = None) -> pd.Series:
    args, kwargs = args_and_kwargs.get('args'), args_and_kwargs.get('kwargs')
    return series.apply(udf, args=args, **kwargs)
