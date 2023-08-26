import pandas as pd
import numpy as np
from pandas.core.groupby.generic import SeriesGroupBy
from pandas.core.window.rolling import Rolling as RollingWindow
from typing import Union, Callable, Iterable, Any

from .window import EmptyWindow, ConcreteWindowSpec
from .indexer import SparkIndexer
from .exceptions import AnalysisException

SeriesType = Union[pd.Series, SeriesGroupBy]
MaybeRollingWindow = Union[
    SeriesGroupBy,
    RollingWindow,
]


def get_rollspec(
    series: pd.Series, bottom: int, top: int, row_or_range="row", range_col=None
) -> SparkIndexer:
    indexer = SparkIndexer(
        series, bottom, top, row_or_range, range_col
    ).get_window_bounds()
    return indexer


def _get_rolling_window(series: pd.Series, *args, **kwargs) -> MaybeRollingWindow:
    win: ConcreteWindowSpec = kwargs.pop("_over")
    convert_to_rolling_sans_order = kwargs.pop("_convert_to_rolling", True)
    df_len = series.size

    if isinstance(win, EmptyWindow):
        return series.rolling(window=df_len, center=True, min_periods=1)
    roll = series.copy()
    if win.partition_by and not win.order_by:
        if win.rows_between or win.range_between:
            raise AnalysisException("Must specify order for row range")
        out = series.groupby(win.partition_by, group_keys=False)
        if convert_to_rolling_sans_order:
            out = out.rolling(window=df_len, center=True, min_periods=1)
        return out
    elif (
        win.partition_by
        and win.order_by
        and not win.rows_between
        and not win.range_between
    ):
        # add all the partition and order columns to a new dataframe, along with
        # the original series. We then return the original series once it's sorted,
        # along with it's nonsorted index if that's requested from the caller.
        ser_name = roll.name
        ob, pb = 0, 0
        rolldf = pd.DataFrame({ser_name: roll})
        for i, order_col in enumerate(win.order_by):
            rolldf[f"__order__{i}"] = order_col
            ob += 1

        for i, partition_col in enumerate(win.partition_by):
            rolldf[f"__partition__{i}"] = partition_col
            pb += 1

        rolldf[ser_name] = series

        # don't reset index here, we might need it later
        rolldf = rolldf.sort_values([f"__order__{i}" for i in range(ob)]).groupby(
            [f"__partition__{i}" for i in range(pb)], group_keys=False
        )

        if convert_to_rolling_sans_order:
            roll = rolldf[ser_name].rolling(window=df_len, center=False, min_periods=1)
        else:
            roll = rolldf[ser_name]
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
    if isinstance(kwargs["_over"], EmptyWindow):
        kwargs.pop("_over")
        return pd.Series(series.sum(*args, **kwargs))
    roll = _get_rolling_window(series, *args, **kwargs)
    try:
        return roll.sum().reset_index()[series.name].astype(series.dtype)
    except pd.errors.IntCastingNaNError:
        return roll.sum().reset_index()[series.name]


def udf_func(
    series: pd.Series,
    udf: Callable,
    args_and_kwargs: dict[Iterable, dict[str, Any]],
    _over=None,
) -> pd.Series:
    args, kwargs = args_and_kwargs.get("args"), args_and_kwargs.get("kwargs")
    return series.apply(udf, args=args, **kwargs)


def sqrt_func(series: pd.Series, *args, **kwargs):
    return pd.Series(np.sqrt(series))


def abs_func(series: pd.Series, *args, **kwargs):
    return pd.Series(np.abs(series))


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


def lower_func(series: pd.Series, *args, **kwargs):
    return series.str.lower()

def upper_func(series: pd.Series, *args, **kwargs):
    return series.str.upper()

def avg_func(series: pd.Series, *args, **kwargs):
    if isinstance(kwargs["_over"], EmptyWindow):
        kwargs.pop("_over")
        return pd.Series(series.mean(*args, **kwargs))
    roll = _get_rolling_window(series, *args, **kwargs)
    return roll.mean().reset_index()[series.name]


def median_func(series: pd.Series, *args, **kwargs):
    pass


def sum_distinct_func(series: pd.Series, *args, **kwargs):
    pass


def product_func(series: pd.Series, *args, **kwargs):
    pass


def acos_func(series: pd.Series, *args, **kwargs):
    pass


def acosh_func(series: pd.Series, *args, **kwargs):
    return pd.Series(np.arccosh(series))


def asin_func(series: pd.Series, *args, **kwargs):
    return pd.Series(np.arcsin(series))


def asinh_func(series: pd.Series, *args, **kwargs):
    return pd.Series(np.arcsinh(series))


def atan_func(series: pd.Series, *args, **kwargs):
    return pd.Series(np.arctan(series))


def atanh_func(series: pd.Series, *args, **kwargs):
    return pd.Series(np.arctanh(series))


def cbrt_func(series: pd.Series, *args, **kwargs):
    return pd.Series(np.cbrt(series))


def ceil_func(series: pd.Series, *args, **kwargs):
    return pd.Series(np.ceil(series))


def cos_func(series: pd.Series, *args, **kwargs):
    return pd.Series(np.cos(series))


def cosh_func(series: pd.Series, *args, **kwargs):
    return pd.Series(np.cosh(series))


def cot_func(series: pd.Series, *args, **kwargs):
    return pd.Series(1 / np.tan(series))


def csc_func(series: pd.Series, *args, **kwargs):
    return pd.Series(1 / np.sin(series))


def exp_func(series: pd.Series, *args, **kwargs):
    pass


def expm1_func(series: pd.Series, *args, **kwargs):
    pass


def floor_func(series: pd.Series, *args, **kwargs):
    return pd.Series(np.floor(series))


def log_func(series: pd.Series, *args, **kwargs):
    return pd.Series(np.log(series))


def log1p_func(series: pd.Series, *args, **kwargs):
    return pd.Series(np.log1p(series))


def rint_func(series: pd.Series, *args, **kwargs):
    pass


def sec_func(series: pd.Series, *args, **kwargs):
    return pd.Series(np.cos(series))


def signum_func(series: pd.Series, *args, **kwargs):
    pass


def sin_func(series: pd.Series, *args, **kwargs):
    return pd.Series(np.sin(series))


def sinh_func(series: pd.Series, *args, **kwargs):
    return pd.Series(np.sinh(series))


def tan_func(series: pd.Series, *args, **kwargs):
    return pd.Series(np.tan(series))


def tanh_func(series: pd.Series, *args, **kwargs):
    return pd.Series(np.tanh(series))


def degrees_func(series: pd.Series, *args, **kwargs):
    return pd.Series(np.degrees(series))


def radians_func(series: pd.Series, *args, **kwargs):
    return pd.Series(np.deg2rad(series))


def bitwise_not_func(series: pd.Series, *args, **kwargs):
    return pd.Series(np.bitwise_not(series))


def asc_func(series: pd.Series, *args, **kwargs):
    pass


def desc_func(series: pd.Series, *args, **kwargs):
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


def hypot_func(series: pd.Series, *args, **kwargs):
    return pd.Series(np.hypot(series))


def pow_func():
    pass


def pmod_func():
    pass


def row_number_func(idx, *args, **kwargs):
    if isinstance(kwargs["_over"], EmptyWindow):
        raise Exception("row_number() is only a Window function") from None
    if not kwargs["_over"].order_by:
        raise AnalysisException("row_number() requires ordered window")
    kwargs["_convert_to_rolling"] = False
    rollspec = _get_rolling_window(idx, *args, **kwargs)
    _rn = rollspec.cumcount() + 1
    return _rn.sort_index()


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
