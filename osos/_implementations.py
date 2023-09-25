import pandas as pd
import numpy as np
from pandas.core.groupby.generic import SeriesGroupBy
from pandas.core.window.rolling import Rolling as RollingWindow
from typing import Union, Callable, Iterable, Any, overload, Optional,List,Tuple,Dict
from warnings import warn

from .window import EmptyWindow, ConcreteWindowSpec
from .indexer import SparkIndexer
from .exceptions import AnalysisException, OsosTypeError, OsosValueError

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
    args_and_kwargs: Dict[Iterable, Dict[str, Any]],
    _over=None,
) -> pd.Series:
    args, kwargs = args_and_kwargs.get("args"), args_and_kwargs.get("kwargs")
    return series.apply(udf, args=args, **kwargs)


def sqrt_func(series: pd.Series, *args, **kwargs):
    return pd.Series(np.sqrt(series))


def abs_func(series: pd.Series, *args, **kwargs):
    return pd.Series(np.abs(series))


def mode_func(series: pd.Series, *args, **kwargs):
    if isinstance(kwargs["_over"], EmptyWindow):
        kwargs.pop("_over")
        return pd.Series(series.mode(*args, **kwargs))
    roll = _get_rolling_window(series, *args, **kwargs)
    try:
        return roll.apply(pd.Series.mode).reset_index()[series.name].astype(series.dtype)
    except pd.errors.IntCastingNaNError:
        return roll.apply(pd.Series.mode).reset_index()[series.name]


def max_func(series: pd.Series, *args, **kwargs):
    if isinstance(kwargs["_over"], EmptyWindow):
        kwargs.pop("_over")
        return pd.Series(series.max(*args, **kwargs))
    roll = _get_rolling_window(series, *args, **kwargs)
    try:
        return roll.max().reset_index()[series.name].astype(series.dtype)
    except pd.errors.IntCastingNaNError:
        return roll.max().reset_index()[series.name]


def min_func(series: pd.Series, *args, **kwargs):
    if isinstance(kwargs["_over"], EmptyWindow):
        kwargs.pop("_over")
        return pd.Series(series.min(*args, **kwargs))
    roll = _get_rolling_window(series, *args, **kwargs)
    try:
        return roll.min().reset_index()[series.name].astype(series.dtype)
    except pd.errors.IntCastingNaNError:
        return roll.min().reset_index()[series.name]


def max_by_func(col: pd.Series, ord: pd.Series, *args, **kwargs):
    if isinstance(kwargs["_over"], EmptyWindow):
        kwargs.pop("_over")
        idxmax = ord.idxmax()
        return col[idxmax]
    else:
        raise AnalysisException("`max_by` cannot be a Window function")


def min_by_func(col: pd.Series, ord: pd.Series, *args, **kwargs):
    if isinstance(kwargs["_over"], EmptyWindow):
        idxmax = ord.idxmax()
        return col[idxmax]
    else:
        raise AnalysisException("`min_by` cannot be a Window function")


def count_func(series: pd.Series, *args, **kwargs):
    if isinstance(kwargs["_over"], EmptyWindow):
        kwargs.pop("_over")
        return pd.Series(series.count(*args, **kwargs))
    roll = _get_rolling_window(series, *args, **kwargs)
    try:
        return roll.count().reset_index()[series.name].astype(series.dtype)
    except pd.errors.IntCastingNaNError:
        return roll.count().reset_index()[series.name]


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
    if isinstance(kwargs["_over"], EmptyWindow):
        kwargs.pop("_over")
        return pd.Series(series.median(*args, **kwargs))
    roll = _get_rolling_window(series, *args, **kwargs)
    try:
        return roll.median().reset_index()[series.name].astype(series.dtype)
    except pd.errors.IntCastingNaNError:
        return roll.median().reset_index()[series.name]


def sum_distinct_func(series: pd.Series, *args, **kwargs):
    if isinstance(kwargs["_over"], EmptyWindow):
        kwargs.pop("_over")
        return pd.Series(series.unique().sum(*args, **kwargs))
    else:
        raise Exception("`sum_distinct` isn't available as a Window function") 


def product_func(series: pd.Series, *args, **kwargs):
    if isinstance(kwargs["_over"], EmptyWindow):
        kwargs.pop("_over")
        return pd.Series(series.product(*args, **kwargs))
    roll = _get_rolling_window(series, *args, **kwargs)
    try:
        return roll.apply(np.prod).reset_index()[series.name].astype(series.dtype)
    except pd.errors.IntCastingNaNError:
        return roll.apply(np.prod).reset_index()[series.name]


def acos_func(series: pd.Series, *args, **kwargs):
    return pd.Series(np.arccos(series))



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
    return pd.Series(np.exp(series))



def expm1_func(series: pd.Series, *args, **kwargs):
    return pd.Series(np.expm1(series))



def floor_func(series: pd.Series, *args, **kwargs):
    return pd.Series(np.floor(series))


def log_func(series: pd.Series, *args, **kwargs):
    return pd.Series(np.log(series))


def log1p_func(series: pd.Series, *args, **kwargs):
    return pd.Series(np.log1p(series))


def rint_func(series: pd.Series, *args, **kwargs):
    return series.round()


def sec_func(series: pd.Series, *args, **kwargs):
    return pd.Series(np.cos(series))


def signum_func(series: pd.Series, *args, **kwargs):
    return pd.Series(np.sign(series))



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
    return series.sort_values(ascending=True)


def desc_func(series: pd.Series, *args, **kwargs):
    return series.sort_values(ascending=False)


def stddev_func(series: pd.Series, *args, **kwargs):
    if isinstance(kwargs["_over"], EmptyWindow):
        kwargs.pop("_over")
        return pd.Series(series.std(*args, **kwargs))
    roll = _get_rolling_window(series, *args, **kwargs)
    try:
        return roll.std().reset_index()[series.name].astype(series.dtype)
    except pd.errors.IntCastingNaNError:
        return roll.std().reset_index()[series.name]


stddev_samp_func = stddev_func


def variance_func(series: pd.Series, *args, **kwargs):
    if isinstance(kwargs["_over"], EmptyWindow):
        kwargs.pop("_over")
        return pd.Series(series.var(*args, **kwargs))
    roll = _get_rolling_window(series, *args, **kwargs)
    try:
        return roll.var().reset_index()[series.name].astype(series.dtype)
    except pd.errors.IntCastingNaNError:
        return roll.var().reset_index()[series.name]


var_samp_func = variance_func


def skewness_func(series: pd.Series, *args, **kwargs):
    if isinstance(kwargs["_over"], EmptyWindow):
        kwargs.pop("_over")
        return pd.Series(series.skew(*args, **kwargs))
    roll = _get_rolling_window(series, *args, **kwargs)
    try:
        return roll.skew().reset_index()[series.name].astype(series.dtype)
    except pd.errors.IntCastingNaNError:
        return roll.skew().reset_index()[series.name]


def kurtosis_func(series: pd.Series, *args, **kwargs):
    if isinstance(kwargs["_over"], EmptyWindow):
        kwargs.pop("_over")
        return pd.Series(series.kurtosis(*args, **kwargs))
    roll = _get_rolling_window(series, *args, **kwargs)
    try:
        return roll.apply(pd.Series.kurtosis).reset_index()[series.name].astype(series.dtype)
    except pd.errors.IntCastingNaNError:
        return roll.apply(pd.Series.kurtosis).reset_index()[series.name]


def atan2_func(col1: pd.Series, col2: pd.Series, *args, **kwargs):
    return pd.Series(np.arctan2(col1, col2))


def hypot_func(series: pd.Series, *args, **kwargs):
    return pd.Series(np.hypot(series))


def pow_func(col1: pd.Series, col2: pd.Series, *args, **kwargs):
    return col1 ** col2


def pmod_func(col1: pd.Series, col2: pd.Series, *args, **kwargs):
    return col1 % col2


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


def coalesce_func(*cols):
    if not len(cols) >= 2:
        raise AnalysisException("Need at least 2 cols to coalesce.")
    out = cols[0]
    for i in range(len(cols) - 1):
        out = np.where(~pd.isnull(out), out, cols[i+1])

    return out

def lag_func(series: pd.Series, *args, **kwargs): 
    if isinstance(kwargs["_over"], EmptyWindow):
        raise AnalysisException("`lag_func` cannot be an Aggregate function")
    roll = _get_rolling_window(series, _convert_to_rolling = False, *args, **kwargs)
    if len(args) == 0:
        offset, default = 1, None
    elif len(args) == 1:
        offset, default = args[0], None
    elif len(args) == 2:
        offset, default = args[0], args[1]
    try:
        return roll.shift(periods = offset, fill_value = default).sort_index().reset_index()[series.name].astype(series.dtype)
    except pd.errors.IntCastingNaNError:
        return roll.shift(periods = offset, fill_value = default).sort_index().reset_index()[series.name]


def lead_func(series: pd.Series, *args, **kwargs):
    if isinstance(kwargs["_over"], EmptyWindow):
        raise AnalysisException("`lead_func` cannot be an Aggregate function")
    roll = _get_rolling_window(series, _convert_to_rolling = False, *args, **kwargs)
    if len(args) == 0:
        offset, default = -1, None
    elif len(args) == 1:
        offset, default = -1*args[0], None
    elif len(args) == 2:
        offset, default = -1*args[0], args[1]
    try:
        return roll.shift(periods = offset, fill_value = default).sort_index().reset_index()[series.name].astype(series.dtype)
    except pd.errors.IntCastingNaNError:
        return roll.shift(periods = offset, fill_value = default).sort_index().reset_index()[series.name]


def corr_func(col1: pd.Series, col2: pd.Series):
    
    raise NotImplementedError



def covar_pop_func(col1: pd.Series, col2: pd.Series):
    
    raise NotImplementedError



def covar_samp_func(col1: pd.Series, col2: pd.Series):
    
    raise NotImplementedError



def countDistinct_func(col: pd.Series, *cols: pd.Series):
    
    return count_distinct_func(col, *cols)



def count_distinct_func(col: pd.Series, *cols: pd.Series):
    
    raise NotImplemented("")



def first_func(col: pd.Series, ignorenulls: bool = False):
    
    raise NotImplementedError



def grouping_func(col: pd.Series):
    
    raise NotImplementedError



def grouping_id(*cols: pd.Series):
    
    raise NotImplementedError



def input_file_name():
    
    raise NotImplementedError



def isnan_func(col: pd.Series):
    
    raise NotImplementedError



def isnull_func(col: pd.Series):
    
    raise NotImplementedError



def last_func(col: pd.Series, ignorenulls: bool = False):
    
    raise NotImplementedError



def monotonically_increasing_id():
    
    raise NotImplementedError



def nanvl_func(col1: pd.Series, col2: pd.Series):
    
    raise NotImplementedError



def percentile_approx_func(
    col: pd.Series,
    percentage: Union[pd.Series, float, List[float], Tuple[float]],
    accuracy: Union[pd.Series, float] = 10000,
):
    

    if isinstance(percentage, pd.Series):
        
        percentage = pd.Series(percentage)
    else:
        
        percentage = pd.Series(percentage)

    accuracy = (
        pd.Series(accuracy)
        if isinstance(accuracy, pd.Series)
        else pd.Series(accuracy)
    )

    raise NotImplementedError



def rand_func(seed: Optional[int] = None):
    
    if seed is not None:
        raise NotImplementedError
    else:
        raise NotImplementedError



def randn_func(seed: Optional[int] = None):
    
    if seed is not None:
        raise NotImplementedError
    else:
        raise NotImplementedError



def round_func(col: pd.Series, scale: int = 0):
    
    raise NotImplementedError



def bround_func(col: pd.Series, scale: int = 0):
    
    raise NotImplementedError



def shiftLeft_func(col: pd.Series, numBits: int):
    
    warn("Deprecated in 3.2, use shiftleft instead.", FutureWarning)
    return shiftleft_func(col, numBits)



def shiftleft_func(col: pd.Series, numBits: int):
    
    raise NotImplementedError



def shiftRight_func(col: pd.Series, numBits: int):
    
    warn("Deprecated in 3.2, use shiftright instead.", FutureWarning)
    return shiftright_func(col, numBits)



def shiftright_func(col: pd.Series, numBits: int):
    
    raise NotImplementedError



def shiftRightUnsigned_func(col: pd.Series, numBits: int):
    
    warn("Deprecated in 3.2, use shiftrightunsigned instead.", FutureWarning)
    return shiftrightunsigned_func(col, numBits)



def shiftrightunsigned_func(col: pd.Series, numBits: int):
    
    raise NotImplementedError



def spark_partition_id():
    
    raise NotImplementedError



def expr(str: str):
    
    raise NotImplementedError


@overload
def struct_func(*cols: pd.Series):
    ...


@overload
def struct_func(
    __cols: Union[List[pd.Series], Tuple[pd.Series, ...]]
):
    ...



def struct_func(
    *cols: Union[
        pd.Series,
        Union[List[pd.Series], Tuple[pd.Series, ...]],
    ]
):
    
    if len(cols) == 1 and isinstance(cols[0], (list, set)):
        cols = cols[0]  
    raise NotImplementedError



def greatest(*cols: pd.Series):
    
    if len(cols) < 2:
        raise OsosValueError(
            error_class="WRONG_NUM_pd.SeriesS",
            message_parameters={"func_name": "greatest", "num_cols": "2"},
        )
    raise NotImplementedError



def least_func(*cols: pd.Series):
    
    if len(cols) < 2:
        raise OsosValueError(
            error_class="WRONG_NUM_pd.SeriesS",
            message_parameters={"func_name": "least", "num_cols": "2"},
        )
    raise NotImplementedError



def when_func(condition: pd.Series, value: Any):
    
    
    if not isinstance(condition, pd.Series):
        raise OsosTypeError(
            error_class="NOT_pd.Series",
            message_parameters={
                "arg_name": "condition",
                "arg_type": type(condition).__name__,
            },
        )
    v = value._jc if isinstance(value, pd.Series) else value

    raise NotImplementedError


@overload  
def log_func(arg1: pd.Series):
    ...


@overload
def log_func(arg1: float, arg2: pd.Series):
    ...



def log_func(
    arg1: Union[pd.Series, float], arg2: Optional[pd.Series] = None
):
    
    if arg2 is None:
        raise NotImplementedError
    else:
        raise NotImplementedError



def log2_func(col: pd.Series):
    
    raise NotImplementedError



def conv_func(col: pd.Series, fromBase: int, toBase: int):
    
    raise NotImplementedError



def factorial_func(col: pd.Series):
    
    raise NotImplementedError



def nth_value_func(
    col: pd.Series, offset: int, ignoreNulls: Optional[bool] = False
):
    
    raise NotImplementedError



def ntile_func(n: int):
    
    raise NotImplementedError


def date_format(date: pd.Series, format: str):
    
    raise NotImplementedError



def year_func(col: pd.Series):
    
    raise NotImplementedError



def quarter_func(col: pd.Series):
    
    raise NotImplementedError



def month_func(col: pd.Series):
    
    raise NotImplementedError



def dayofweek_func(col: pd.Series):
    
    raise NotImplementedError



def dayofmonth_func(col: pd.Series):
    
    raise NotImplementedError



def dayofyear_func(col: pd.Series):
    
    raise NotImplementedError



def hour_func(col: pd.Series):
    
    raise NotImplementedError



def minute_func(col: pd.Series):
    
    raise NotImplementedError



def second_func(col: pd.Series):
    
    raise NotImplementedError



def weekofyear_func(col: pd.Series):
    
    raise NotImplementedError



def make_date_func(
    year: pd.Series, month: pd.Series, day: pd.Series
):
    
    raise NotImplementedError



def date_add_func(start: pd.Series, days: Union[pd.Series, int]):
    
    days = pd.Series(days) if isinstance(days, int) else days
    raise NotImplementedError



def date_sub_func(start: pd.Series, days: Union[pd.Series, int]):
    
    days = pd.Series(days) if isinstance(days, int) else days
    raise NotImplementedError



def datediff_func(end: pd.Series, start: pd.Series):
    
    raise NotImplementedError



def add_months_func(
    start: pd.Series, months: Union[pd.Series, int]
):
    
    months = pd.Series(months) if isinstance(months, int) else months
    raise NotImplementedError



def months_between_func(
    date1: pd.Series, date2: pd.Series, roundOff: bool = True
):
    
    raise NotImplemented(
        "months_between", pd.Series(date1), pd.Series(date2), roundOff
    )



def to_date_func(col: pd.Series, format: Optional[str] = None):
    
    if format is None:
        raise NotImplementedError
    else:
        raise NotImplementedError


@overload
def to_timestamp_func(col: pd.Series):
    ...


@overload
def to_timestamp_func(col: pd.Series, format: str):
    ...



def to_timestamp_func(col: pd.Series, format: Optional[str] = None):
    
    if format is None:
        raise NotImplementedError
    else:
        raise NotImplementedError



def trunc_func(date: pd.Series, format: str):
    
    raise NotImplementedError



def date_trunc_func(format: str, timestamp: pd.Series):
    
    raise NotImplementedError



def next_day_func(date: pd.Series, dayOfWeek: str):
    
    raise NotImplementedError



def last_day_func(date: pd.Series):
    
    raise NotImplementedError



def from_unixtime_func(
    timestamp: pd.Series, format: str = "yyyy-MM-dd HH:mm:ss"
):
    
    raise NotImplementedError


@overload
def unix_timestamp_func(timestamp: pd.Series, format: str = ...):
    ...


@overload
def unix_timestamp_func():
    ...



def unix_timestamp_func(
    timestamp: Optional[pd.Series] = None, format: str = "yyyy-MM-dd HH:mm:ss"
):
    
    if timestamp is None:
        raise NotImplementedError
    raise NotImplementedError



def from_utc_timestamp_func(timestamp: pd.Series, tz: pd.Series):
    
    if isinstance(tz, pd.Series):
        tz = pd.Series(tz)
    raise NotImplementedError



def to_utc_timestamp_func(timestamp: pd.Series, tz: pd.Series):
    
    if isinstance(tz, pd.Series):
        tz = pd.Series(tz)
    raise NotImplementedError



def timestamp_seconds_func(col: pd.Series):
    

    raise NotImplementedError



def window_func(
    series: pd.Series,
    windowDuration: str,
    slideDuration: Optional[str] = None,
    startTime: Optional[str] = None,
):
    raise NotImplementedError



def window_time_func(
    window: pd.Series,
):
    raise NotImplementedError




def crc32_func(col: pd.Series):
    
    raise NotImplementedError



def md5_func(col: pd.Series):
    
    raise NotImplementedError



def sha1_func(col: pd.Series):
    
    raise NotImplementedError



def sha2_func(col: pd.Series, numBits: int):
    
    raise NotImplementedError



def hash_func(*cols: pd.Series):
    
    raise NotImplementedError



def xxhash64_func(*cols: pd.Series):
    
    raise NotImplementedError



def assert_true_func(
    col: pd.Series, errMsg: Optional[Union[pd.Series, str]] = None
):
    
    if errMsg is None:
        raise NotImplementedError
    if not isinstance(errMsg, (str, pd.Series)):
        raise OsosTypeError(
            error_class="NOT_pd.Series_OR_STR",
            message_parameters={
                "arg_name": "errMsg",
                "arg_type": type(errMsg).__name__,
            },
        )

    errMsg = (
       "foo"
        if isinstance(errMsg, str)
        else pd.Series(errMsg)
    )
    raise NotImplementedError



def raise_error_func(errMsg: Union[pd.Series, str]):
    
    if not isinstance(errMsg, (str, pd.Series)):
        raise OsosTypeError(
            error_class="NOT_pd.Series_OR_STR",
            message_parameters={
                "arg_name": "errMsg",
                "arg_type": type(errMsg).__name__,
            },
        )

    errMsg = pd.Series(errMsg) if isinstance(errMsg, str) else pd.Series(errMsg)
    raise NotImplementedError



def ascii_func(col: pd.Series):
    
    raise NotImplementedError



def base64_func(col: pd.Series):
    
    raise NotImplementedError



def unbase64_func(col: pd.Series):
    
    raise NotImplementedError



def ltrim_func(col: pd.Series):
    
    raise NotImplementedError



def rtrim_func(col: pd.Series):
    
    raise NotImplementedError



def trim_func(col: pd.Series):
    
    raise NotImplementedError



def concat_ws(sep: str, *cols: pd.Series):
    
    raise NotImplementedError



def decode_func(col: pd.Series, charset: str):
    
    raise NotImplementedError



def encode_func(col: pd.Series, charset: str):
    
    raise NotImplementedError



def format_number_func(col: pd.Series, d: int):
    
    raise NotImplementedError



def format_string_func(format: str, *cols: pd.Series):
    
    raise NotImplementedError



def instr_func(str: pd.Series, substr: str):
    
    raise NotImplementedError



def overlay_func(
    src: pd.Series,
    replace: pd.Series,
    pos: Union[pd.Series, int],
    len: Union[pd.Series, int] = -1,
):
    
    if not isinstance(pos, (int, str, pd.Series)):
        raise OsosTypeError(
            error_class="NOT_pd.Series_OR_INT_OR_STR",
            message_parameters={"arg_name": "pos", "arg_type": type(pos).__name__},
        )
    if len is not None and not isinstance(len, (int, str, pd.Series)):
        raise OsosTypeError(
            error_class="NOT_pd.Series_OR_INT_OR_STR",
            message_parameters={"arg_name": "len", "arg_type": type(len).__name__},
        )

    pos = pd.Series(pos) if isinstance(pos, int) else pd.Series(pos)
    len = pd.Series(len) if isinstance(len, int) else pd.Series(len)

    raise NotImplementedError



def sentences_func(
    string: pd.Series,
    language: Optional[pd.Series] = None,
    country: Optional[pd.Series] = None,
):
    
    if language is None:
        language = pd.Series("")
    if country is None:
        country = pd.Series("")

    raise NotImplementedError



def substring_func(str: pd.Series, pos: int, len: int):
    
    raise NotImplementedError



def substring_index_func(str: pd.Series, delim: str, count: int):
    
    raise NotImplementedError



def levenshtein_Func(left: pd.Series, right: pd.Series):
    
    raise NotImplementedError



def locate_func(substr: str, str: pd.Series, pos: int = 1):
    
    raise NotImplementedError



def lpad_func(col: pd.Series, len: int, pad: str):
    
    raise NotImplementedError



def rpad_func(col: pd.Series, len: int, pad: str):
    
    raise NotImplementedError



def repeat_func(col: pd.Series, n: int):
    
    raise NotImplementedError



def split_func(str: pd.Series, pattern: str, limit: int = -1):
    
    raise NotImplementedError



def regexp_extract_func(str: pd.Series, pattern: str, idx: int):
    
    raise NotImplementedError



def regexp_replace_func(
    string: pd.Series,
    pattern: Union[str, pd.Series],
    replacement: Union[str, pd.Series],
):
    
    if isinstance(pattern, str):
        pattern_col = pd.Series(pattern)
    else:
        pattern_col = pd.Series(pattern)
    if isinstance(replacement, str):
        replacement_col = pd.Series(replacement)
    else:
        replacement_col = pd.Series(replacement)
    raise NotImplementedError



def initcap_func(col: pd.Series):
    
    raise NotImplementedError



def soundex_func(col: pd.Series):
    
    raise NotImplementedError



def bin_func(col: pd.Series):
    
    raise NotImplementedError



def hex_func(col: pd.Series):
    
    raise NotImplementedError



def unhex_func(col: pd.Series):
    
    raise NotImplementedError



def length_func(col: pd.Series):
    
    raise NotImplementedError



def octet_length_func(col: pd.Series):
    
    raise NotImplementedError



def bit_length_func(col: pd.Series):
    
    raise NotImplementedError



def translate_func(srcCol: pd.Series, matching: str, replace: str):
    
    raise NotImplementedError