import pandas as pd
import numpy as np
from pandas.core.groupby.generic import SeriesGroupBy
from typing import Union, Callable, Iterable, Mapping, Any
import itertools

from .window import EmptyWindow,MAX_WINDOW_SIZE
from .indexer import SparkIndexer
SeriesType = Union[pd.Series,SeriesGroupBy]

def get_rollspec(series: pd.Series,bottom: int, top: int,row_or_range='row',range_col=None) -> SparkIndexer:
    indexer = SparkIndexer(series,bottom,top,row_or_range,range_col).get_window_bounds()
    return indexer

def _get_rolling_window(series: pd.Series, *args, **kwargs):
    win = kwargs.pop("_over")
    df_len = series.size
    if isinstance(win,EmptyWindow):
        return series.rolling(window=df_len,center=True,min_periods=1)
    roll = series.copy()
    if win.partition_by:
        roll = series.groupby(win.partition_by).rolling(window=df_len,center=True,min_periods=1)
    # if win.order_by:
    #     indices = np.argsort(win.order_by)
    #     roll = roll.sort_values(indices) # this is wrong
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
    if isinstance(kwargs['_over'], EmptyWindow):
        kwargs.pop("_over")
        return pd.Series(series.sum(*args,**kwargs))
    roll = _get_rolling_window(series, *args, **kwargs)
    try:
        return roll.sum().reset_index()[series.name].astype(series.dtype)
    except pd.errors.IntCastingNaNError:
        return roll.sum().reset_index()[series.name]




def udf_func(series: pd.Series, udf: Callable, args_and_kwargs: dict[Iterable, dict[str,Any]], _over = None) -> pd.Series:
    args, kwargs = args_and_kwargs.get('args'), args_and_kwargs.get('kwargs')
    return series.apply(udf, args=args, **kwargs)

def sqrt_func():
    pass

def abs_func():
    pass

def mode_func():
    pass

def max_func():
    pass

def min_func():
    pass

def max_by_func():
    pass

def min_by_func():
    pass

def count_func():
    pass

def avg_func():
    pass

def median_func():
    pass

def sum_distinct_func():
    pass

def product_func():
    pass

def acos_func():
    pass

def acosh_func():
    pass

def asin_func():
    pass

def asinh_func():
    pass

def atan_func():
    pass

def atanh_func():
    pass

def cbrt_func():
    pass

def ceil_func():
    pass

def cos_func():
    pass

def cosh_func():
    pass

def cot_func():
    pass

def csc_func():
    pass

def exp_func():
    pass

def expm1_func():
    pass

def floor_func():
    pass

def log_func():
    pass

def log1p_func():
    pass

def rint_func():
    pass

def sec_func():
    pass

def signum_func():
    pass

def sin_func():
    pass

def sinh_func():
    pass

def tan_func():
    pass

def tanh_func():
    pass

def degrees_func():
    pass

def radians_func():
    pass

def bitwise_not_func():
    pass

def asc_func():
    pass

def desc_func():
    pass

def stdev_func():
    pass

def stdev_samp_func():
    pass

def variance_func():
    pass

def var_samp_func():
    pass

def skewness_func():
    pass

def kurtosis_func():
    pass

def atan2_func():
    pass

def hypot_func():
    pass

def pow_func():
    pass

def pmod_func():
    pass

def row_number_func():
    pass

def dense_rank_func():
    pass

def rank_func():
    pass

def cume_dist_func():
    pass

def percent_rank_func():
    pass

def approx_count_distinct_func():
    pass

def coalesce_func():
    pass