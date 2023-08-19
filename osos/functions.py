from .column import (
    AbstractCol,
    AbstractColOrName,
    AbstractLit,
    AbstractColOrLit,
    Func,
    ArbitraryFunction,
    ArgList,
    AbstractIndex,
    Func,
)
from .exceptions import AnalysisException, OsosValueError, OsosTypeError
from .dataframe import DataFrame


from typing import Any, Optional, Union, List, Tuple, overload, Callable
from warnings import warn

# all the public names in _implementations end in "_func"
from ._implementations import *


def _to_seq(iterable, iter_type):
    return [iter_type(i) for i in iterable]


def col(name: str):
    return AbstractCol(name)


try_remote_functions = lambda callable: callable


@try_remote_functions
def lit(col: Any) -> Func:
    """
    Creates a :class:`~osos.Col` of literal value.

    .. versionadded:: 1.3.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col`, str, int, float, bool or list, NumPy literals or ndarray.
        the value to make it as a PySpark literal. If a AbstractCol is passed,
        it returns the AbstractCol as is.

        .. versionchanged:: 3.4.0
            Since 3.4.0, it supports the list type.

    Returns
    -------
    :class:`~osos.Col`
        the literal instance.

    Examples
    --------
    >>> df = spark.range(1)
    >>> df.select(lit(5).alias('height'), df.id).show()
    +------+---+
    |height| id|
    +------+---+
    |     5|  0|
    +------+---+

    Create a literal from a list.

    >>> spark.range(1).select(lit([1, 2, 3])).show()
    +--------------+
    |array(1, 2, 3)|
    +--------------+
    |     [1, 2, 3]|
    +--------------+
    """
    return AbstractLit(col)


def col(col: str) -> Func:
    """
    Returns a :class:`~osos.Col` based on the given AbstractCol name.

    .. versionadded:: 1.3.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : str
        the name for the AbstractCol

    Returns
    -------
    :class:`~osos.Col`
        the corresponding AbstractCol instance.

    Examples
    --------
    >>> col('x')
    AbstractCol<'x'>
    >>> AbstractCol('x')
    AbstractCol<'x'>
    """
    return AbstractCol(col)


def asc(col: "AbstractColOrName") -> Func:
    """
    Returns a sort expression based on the ascending order of the given AbstractCol name.

    .. versionadded:: 1.3.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        target AbstractCol to sort by in the ascending order.

    Returns
    -------
    :class:`~osos.Col`
        the AbstractCol specifying the order.

    Examples
    --------
    Sort by the AbstractCol 'id' in the descending order.

    >>> df = spark.range(5)
    >>> df = df.sort(desc("id"))
    >>> df.show()
    +---+
    | id|
    +---+
    |  4|
    |  3|
    |  2|
    |  1|
    |  0|
    +---+

    Sort by the AbstractCol 'id' in the ascending order.

    >>> df.orderBy(asc("id")).show()
    +---+
    | id|
    +---+
    |  0|
    |  1|
    |  2|
    |  3|
    |  4|
    +---+
    """
    raise NotImplementedError


@try_remote_functions
def desc(col: "AbstractColOrName") -> Func:
    """
    Returns a sort expression based on the descending order of the given AbstractCol name.

    .. versionadded:: 1.3.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        target AbstractCol to sort by in the descending order.

    Returns
    -------
    :class:`~osos.Col`
        the AbstractCol specifying the order.

    Examples
    --------
    Sort by the AbstractCol 'id' in the descending order.

    >>> spark.range(5).orderBy(desc("id")).show()
    +---+
    | id|
    +---+
    |  4|
    |  3|
    |  2|
    |  1|
    |  0|
    +---+
    """
    raise NotImplementedError


@try_remote_functions
def sqrt(col: "AbstractColOrName") -> Func:
    """
    Computes the square root of the specified float value.

    .. versionadded:: 1.3.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        target AbstractCol to compute on.

    Returns
    -------
    :class:`~osos.Col`
        AbstractCol for computed results.

    Examples
    --------
    >>> df = spark.range(1)
    >>> df.select(sqrt(lit(4))).show()
    +-------+
    |SQRT(4)|
    +-------+
    |    2.0|
    +-------+
    """
    if isinstance(col, str):
        col = AbstractCol(col)
    return Func(sqrt_func, col)


@try_remote_functions
def abs(col: "AbstractColOrName") -> Func:
    """
    Computes the absolute value.

    .. versionadded:: 1.3.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        target AbstractCol to compute on.

    Returns
    -------
    :class:`~osos.Col`
        AbstractCol for computed results.

    Examples
    --------
    >>> df = spark.range(1)
    >>> df.select(abs(lit(-1))).show()
    +-------+
    |abs(-1)|
    +-------+
    |      1|
    +-------+
    """
    if isinstance(col, str):
        col = AbstractCol(col)
    return Func(abs_func, col)


@try_remote_functions
def mode(col: "AbstractColOrName") -> Func:
    """
    Returns the most frequent value in a group.

    .. versionadded:: 3.4.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        target AbstractCol to compute on.

    Returns
    -------
    :class:`~osos.Col`
        the most frequent value in a group.

    Examples
    --------
    >>> df = spark.createDataFrame([
    ...     ("Java", 2012, 20000), ("dotNET", 2012, 5000),
    ...     ("Java", 2012, 20000), ("dotNET", 2012, 5000),
    ...     ("dotNET", 2013, 48000), ("Java", 2013, 30000)],
    ...     schema=("course", "year", "earnings"))
    >>> df.groupby("course").agg(mode("year")).show()
    +------+----------+
    |course|mode(year)|
    +------+----------+
    |  Java|      2012|
    |dotNET|      2012|
    +------+----------+
    """
    return Func(mode_func, col)


@try_remote_functions
def max(col: "AbstractColOrName") -> Func:
    """
    Aggregate function: returns the maximum value of the expression in a group.

    .. versionadded:: 1.3.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        target AbstractCol to compute on.

    Returns
    -------
    :class:`~osos.Col`
        AbstractCol for computed results.

    Examples
    --------
    >>> df = spark.range(10)
    >>> df.select(max(col("id"))).show()
    +-------+
    |max(id)|
    +-------+
    |      9|
    +-------+
    """
    return Func(max_func, col)


@try_remote_functions
def min(col: "AbstractColOrName") -> Func:
    """
    Aggregate function: returns the minimum value of the expression in a group.

    .. versionadded:: 1.3.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        target AbstractCol to compute on.

    Returns
    -------
    :class:`~osos.Col`
        AbstractCol for computed results.

    Examples
    --------
    >>> df = spark.range(10)
    >>> df.select(min(df.id)).show()
    +-------+
    |min(id)|
    +-------+
    |      0|
    +-------+
    """
    if isinstance(col, str):
        col = AbstractCol(col)

    return Func(min_func, col)


@try_remote_functions
def max_by(col: "AbstractColOrName", ord: "AbstractColOrName") -> Func:
    """
    Returns the value associated with the maximum value of ord.

    .. versionadded:: 3.3.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        target AbstractCol to compute on.
    ord : :class:`~osos.Col` or str
        AbstractCol to be maximized

    Returns
    -------
    :class:`~osos.Col`
        value associated with the maximum value of ord.

    Examples
    --------
    >>> df = spark.createDataFrame([
    ...     ("Java", 2012, 20000), ("dotNET", 2012, 5000),
    ...     ("dotNET", 2013, 48000), ("Java", 2013, 30000)],
    ...     schema=("course", "year", "earnings"))
    >>> df.groupby("course").agg(max_by("year", "earnings")).show()
    +------+----------------------+
    |course|max_by(year, earnings)|
    +------+----------------------+
    |  Java|                  2013|
    |dotNET|                  2013|
    +------+----------------------+
    """
    if isinstance(col, str):
        col = AbstractCol(col)

    return Func(max_by_func, col, ord)


@try_remote_functions
def min_by(col: "AbstractColOrName", ord: "AbstractColOrName") -> Func:
    """
    Returns the value associated with the minimum value of ord.

    .. versionadded:: 3.3.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        target AbstractCol to compute on.
    ord : :class:`~osos.Col` or str
        AbstractCol to be minimized

    Returns
    -------
    :class:`~osos.Col`
        value associated with the minimum value of ord.

    Examples
    --------
    >>> df = spark.createDataFrame([
    ...     ("Java", 2012, 20000), ("dotNET", 2012, 5000),
    ...     ("dotNET", 2013, 48000), ("Java", 2013, 30000)],
    ...     schema=("course", "year", "earnings"))
    >>> df.groupby("course").agg(min_by("year", "earnings")).show()
    +------+----------------------+
    |course|min_by(year, earnings)|
    +------+----------------------+
    |  Java|                  2012|
    |dotNET|                  2012|
    +------+----------------------+
    """
    if isinstance(col, str):
        col = AbstractCol(col)

    return Func(min_by_func, col, ord)


@try_remote_functions
def count(col: "AbstractColOrName") -> Func:
    """
    Aggregate function: returns the number of items in a group.

    .. versionadded:: 1.3.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        target AbstractCol to compute on.

    Returns
    -------
    :class:`~osos.Col`
        AbstractCol for computed results.

    Examples
    --------
    Count by all AbstractCols (start), and by a AbstractCol that does not count ``None``.

    >>> df = spark.createDataFrame([(None,), ("a",), ("b",), ("c",)], schema=["alphabets"])
    >>> df.select(count(expr("*")), count(df.alphabets)).show()
    +--------+----------------+
    |count(1)|count(alphabets)|
    +--------+----------------+
    |       4|               3|
    +--------+----------------+
    """
    if isinstance(col, str):
        col = AbstractCol(col)

    return Func(count_func, col)


@try_remote_functions
def sum(col: "AbstractColOrName") -> Func:
    """
    Aggregate function: returns the sum of all values in the expression.

    .. versionadded:: 1.3.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        target AbstractCol to compute on.

    Returns
    -------
    :class:`~osos.Col`
        the AbstractCol for computed results.

    Examples
    --------
    >>> df = spark.range(10)
    >>> df.select(sum(df["id"])).show()
    +-------+
    |sum(id)|
    +-------+
    |     45|
    +-------+
    """
    if isinstance(col, str):
        col = AbstractCol(col)

    return Func(sum_func, col)


@try_remote_functions
def avg(col: "AbstractColOrName") -> Func:
    """
    Aggregate function: returns the average of the values in a group.

    .. versionadded:: 1.3.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        target AbstractCol to compute on.

    Returns
    -------
    :class:`~osos.Col`
        the AbstractCol for computed results.

    Examples
    --------
    >>> df = spark.range(10)
    >>> df.select(avg(col("id"))).show()
    +-------+
    |avg(id)|
    +-------+
    |    4.5|
    +-------+
    """
    if isinstance(col, str):
        col = AbstractCol(col)

    return Func(avg_func, col)


@try_remote_functions
def mean(col: "AbstractColOrName") -> Func:
    """
    Aggregate function: returns the average of the values in a group.
    An alias of :func:`avg`.

    .. versionadded:: 1.4.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        target AbstractCol to compute on.

    Returns
    -------
    :class:`~osos.Col`
        the AbstractCol for computed results.

    Examples
    --------
    >>> df = spark.range(10)
    >>> df.select(mean(df.id)).show()
    +-------+
    |avg(id)|
    +-------+
    |    4.5|
    +-------+
    """
    if isinstance(col, str):
        col = AbstractCol(col)

    return Func(avg_func, col)


@try_remote_functions
def median(col: "AbstractColOrName") -> Func:
    """
    Returns the median of the values in a group.

    .. versionadded:: 3.4.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        target AbstractCol to compute on.

    Returns
    -------
    :class:`~osos.Col`
        the median of the values in a group.

    Examples
    --------
    >>> df = spark.createDataFrame([
    ...     ("Java", 2012, 20000), ("dotNET", 2012, 5000),
    ...     ("Java", 2012, 22000), ("dotNET", 2012, 10000),
    ...     ("dotNET", 2013, 48000), ("Java", 2013, 30000)],
    ...     schema=("course", "year", "earnings"))
    >>> df.groupby("course").agg(median("earnings")).show()
    +------+----------------+
    |course|median(earnings)|
    +------+----------------+
    |  Java|         22000.0|
    |dotNET|         10000.0|
    +------+----------------+
    """
    if isinstance(col, str):
        col = AbstractCol(col)

    return Func(median_func, col)


@try_remote_functions
def sumDistinct(col: "AbstractColOrName") -> Func:
    """
    Aggregate function: returns the sum of distinct values in the expression.

    .. versionadded:: 1.3.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    .. deprecated:: 3.2.0
        Use :func:`sum_distinct` instead.
    """
    warn("Deprecated in 3.2, use sum_distinct instead.", FutureWarning)
    if isinstance(col, str):
        col = AbstractCol(col)

    return Func(sum_distinct_func, col)


@try_remote_functions
def sum_distinct(col: "AbstractColOrName") -> Func:
    """
    Aggregate function: returns the sum of distinct values in the expression.

    .. versionadded:: 3.2.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        target AbstractCol to compute on.

    Returns
    -------
    :class:`~osos.Col`
        the AbstractCol for computed results.

    Examples
    --------
    >>> df = spark.createDataFrame([(None,), (1,), (1,), (2,)], schema=["numbers"])
    >>> df.select(sum_distinct(col("numbers"))).show()
    +---------------------+
    |sum(DISTINCT numbers)|
    +---------------------+
    |                    3|
    +---------------------+
    """
    if isinstance(col, str):
        col = AbstractCol(col)

    return Func(sum_distict_func, col)


@try_remote_functions
def product(col: "AbstractColOrName") -> Func:
    """
    Aggregate function: returns the product of the values in a group.

    .. versionadded:: 3.2.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : str, :class:`AbstractCol`
        AbstractCol containing values to be multiplied together

    Returns
    -------
    :class:`~osos.Col`
        the AbstractCol for computed results.

    Examples
    --------
    >>> df = spark.range(1, 10).toDF('x').withAbstractCol('mod3', col('x') % 3)
    >>> prods = df.groupBy('mod3').agg(product('x').alias('product'))
    >>> prods.orderBy('mod3').show()
    +----+-------+
    |mod3|product|
    +----+-------+
    |   0|  162.0|
    |   1|   28.0|
    |   2|   80.0|
    +----+-------+
    """
    if isinstance(col, str):
        col = AbstractCol(col)

    return Func(product_func, col)


@try_remote_functions
def acos(col: "AbstractColOrName") -> Func:
    """
    Computes inverse cosine of the input AbstractCol.

    .. versionadded:: 1.4.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        target AbstractCol to compute on.

    Returns
    -------
    :class:`~osos.Col`
        inverse cosine of `col`, as if computed by `java.lang.Math.acos()`

    Examples
    --------
    >>> df = spark.range(1, 3)
    >>> df.select(acos(df.id)).show()
    +--------+
    |ACOS(id)|
    +--------+
    |     0.0|
    |     NaN|
    +--------+
    """
    if isinstance(col, str):
        col = AbstractCol(col)

    return Func(acos_func, col)


@try_remote_functions
def acosh(col: "AbstractColOrName") -> Func:
    """
    Computes inverse hyperbolic cosine of the input AbstractCol.

    .. versionadded:: 3.1.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        target AbstractCol to compute on.

    Returns
    -------
    :class:`~osos.Col`
        the AbstractCol for computed results.

    Examples
    --------
    >>> df = spark.range(2)
    >>> df.select(acosh(col("id"))).show()
    +---------+
    |ACOSH(id)|
    +---------+
    |      NaN|
    |      0.0|
    +---------+
    """
    if isinstance(col, str):
        col = AbstractCol(col)

    return Func(acosh_func, col)


@try_remote_functions
def asin(col: "AbstractColOrName") -> Func:
    """
    Computes inverse sine of the input AbstractCol.

    .. versionadded:: 1.4.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        target AbstractCol to compute on.

    Returns
    -------
    :class:`~osos.Col`
        inverse sine of `col`, as if computed by `java.lang.Math.asin()`

    Examples
    --------
    >>> df = spark.createDataFrame([(0,), (2,)])
    >>> df.select(asin(df.schema.fieldNames()[0])).show()
    +--------+
    |ASIN(_1)|
    +--------+
    |     0.0|
    |     NaN|
    +--------+
    """
    if isinstance(col, str):
        col = AbstractCol(col)

    return Func(asin_func, col)


@try_remote_functions
def asinh(col: "AbstractColOrName") -> Func:
    """
    Computes inverse hyperbolic sine of the input AbstractCol.

    .. versionadded:: 3.1.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        target AbstractCol to compute on.

    Returns
    -------
    :class:`~osos.Col`
        the AbstractCol for computed results.

    Examples
    --------
    >>> df = spark.range(1)
    >>> df.select(asinh(col("id"))).show()
    +---------+
    |ASINH(id)|
    +---------+
    |      0.0|
    +---------+
    """
    if isinstance(col, str):
        col = AbstractCol(col)

    return Func(asinh_func, col)


@try_remote_functions
def atan(col: "AbstractColOrName") -> Func:
    """
    Compute inverse tangent of the input AbstractCol.

    .. versionadded:: 1.4.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        target AbstractCol to compute on.

    Returns
    -------
    :class:`~osos.Col`
        inverse tangent of `col`, as if computed by `java.lang.Math.atan()`

    Examples
    --------
    >>> df = spark.range(1)
    >>> df.select(atan(df.id)).show()
    +--------+
    |ATAN(id)|
    +--------+
    |     0.0|
    +--------+
    """
    if isinstance(col, str):
        col = AbstractCol(col)

    return Func(atan_func, col)


@try_remote_functions
def atanh(col: "AbstractColOrName") -> Func:
    """
    Computes inverse hyperbolic tangent of the input AbstractCol.

    .. versionadded:: 3.1.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        target AbstractCol to compute on.

    Returns
    -------
    :class:`~osos.Col`
        the AbstractCol for computed results.

    Examples
    --------
    >>> df = spark.createDataFrame([(0,), (2,)], schema=["numbers"])
    >>> df.select(atanh(df["numbers"])).show()
    +--------------+
    |ATANH(numbers)|
    +--------------+
    |           0.0|
    |           NaN|
    +--------------+
    """
    if isinstance(col, str):
        col = AbstractCol(col)

    return Func(atanh_func, col)


@try_remote_functions
def cbrt(col: "AbstractColOrName") -> Func:
    """
    Computes the cube-root of the given value.

    .. versionadded:: 1.4.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        target AbstractCol to compute on.

    Returns
    -------
    :class:`~osos.Col`
        the AbstractCol for computed results.

    Examples
    --------
    >>> df = spark.range(1)
    >>> df.select(cbrt(lit(27))).show()
    +--------+
    |CBRT(27)|
    +--------+
    |     3.0|
    +--------+
    """
    if isinstance(col, str):
        col = AbstractCol(col)

    return Func(cbrt_func, col)


@try_remote_functions
def ceil(col: "AbstractColOrName") -> Func:
    """
    Computes the ceiling of the given value.

    .. versionadded:: 1.4.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        target AbstractCol to compute on.

    Returns
    -------
    :class:`~osos.Col`
        the AbstractCol for computed results.

    Examples
    --------
    >>> df = spark.range(1)
    >>> df.select(ceil(lit(-0.1))).show()
    +----------+
    |CEIL(-0.1)|
    +----------+
    |         0|
    +----------+
    """
    if isinstance(col, str):
        col = AbstractCol(col)

    return Func(ceil_func, col)


@try_remote_functions
def cos(col: "AbstractColOrName") -> Func:
    """
    Computes cosine of the input AbstractCol.

    .. versionadded:: 1.4.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        angle in radians

    Returns
    -------
    :class:`~osos.Col`
        cosine of the angle, as if computed by `java.lang.Math.cos()`.

    Examples
    --------
    >>> import math
    >>> df = spark.range(1)
    >>> df.select(cos(lit(math.pi))).first()
    Row(COS(3.14159...)=-1.0)
    """
    if isinstance(col, str):
        col = AbstractCol(col)

    return Func(cos_func, col)


@try_remote_functions
def cosh(col: "AbstractColOrName") -> Func:
    """
    Computes hyperbolic cosine of the input AbstractCol.

    .. versionadded:: 1.4.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        hyperbolic angle

    Returns
    -------
    :class:`~osos.Col`
        hyperbolic cosine of the angle, as if computed by `java.lang.Math.cosh()`

    Examples
    --------
    >>> df = spark.range(1)
    >>> df.select(cosh(lit(1))).first()
    Row(COSH(1)=1.54308...)
    """
    if isinstance(col, str):
        col = AbstractCol(col)

    return Func(cosh_func, col)


@try_remote_functions
def cot(col: "AbstractColOrName") -> Func:
    """
    Computes cotangent of the input AbstractCol.

    .. versionadded:: 3.3.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        angle in radians.

    Returns
    -------
    :class:`~osos.Col`
        cotangent of the angle.

    Examples
    --------
    >>> import math
    >>> df = spark.range(1)
    >>> df.select(cot(lit(math.radians(45)))).first()
    Row(COT(0.78539...)=1.00000...)
    """
    if isinstance(col, str):
        col = AbstractCol(col)

    return Func(cot_func, col)


@try_remote_functions
def csc(col: "AbstractColOrName") -> Func:
    """
    Computes cosecant of the input AbstractCol.

    .. versionadded:: 3.3.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        angle in radians.

    Returns
    -------
    :class:`~osos.Col`
        cosecant of the angle.

    Examples
    --------
    >>> import math
    >>> df = spark.range(1)
    >>> df.select(csc(lit(math.radians(90)))).first()
    Row(CSC(1.57079...)=1.0)
    """
    if isinstance(col, str):
        col = AbstractCol(col)

    return Func(csc_func, col)


@try_remote_functions
def exp(col: "AbstractColOrName") -> Func:
    """
    Computes the exponential of the given value.

    .. versionadded:: 1.4.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        AbstractCol to calculate exponential for.

    Returns
    -------
    :class:`~osos.Col`
        exponential of the given value.

    Examples
    --------
    >>> df = spark.range(1)
    >>> df.select(exp(lit(0))).show()
    +------+
    |EXP(0)|
    +------+
    |   1.0|
    +------+
    """
    if isinstance(col, str):
        col = AbstractCol(col)

    return Func(exp_func, col)


@try_remote_functions
def expm1(col: "AbstractColOrName") -> Func:
    """
    Computes the exponential of the given value minus one.

    .. versionadded:: 1.4.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        AbstractCol to calculate exponential for.

    Returns
    -------
    :class:`~osos.Col`
        exponential less one.

    Examples
    --------
    >>> df = spark.range(1)
    >>> df.select(expm1(lit(1))).first()
    Row(EXPM1(1)=1.71828...)
    """
    if isinstance(col, str):
        col = AbstractCol(col)

    return Func(expm1_func, col)


@try_remote_functions
def floor(col: "AbstractColOrName") -> Func:
    """
    Computes the floor of the given value.

    .. versionadded:: 1.4.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        AbstractCol to find floor for.

    Returns
    -------
    :class:`~osos.Col`
        nearest integer that is less than or equal to given value.

    Examples
    --------
    >>> df = spark.range(1)
    >>> df.select(floor(lit(2.5))).show()
    +----------+
    |FLOOR(2.5)|
    +----------+
    |         2|
    +----------+
    """
    if isinstance(col, str):
        col = AbstractCol(col)

    return Func(floor_func, col)


@try_remote_functions
def log(col: "AbstractColOrName") -> Func:
    """
    Computes the natural logarithm of the given value.

    .. versionadded:: 1.4.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        AbstractCol to calculate natural logarithm for.

    Returns
    -------
    :class:`~osos.Col`
        natural logarithm of the given value.

    Examples
    --------
    >>> import math
    >>> df = spark.range(1)
    >>> df.select(log(lit(math.e))).first()
    Row(ln(2.71828...)=1.0)
    """
    if isinstance(col, str):
        col = AbstractCol(col)

    return Func(log_func, col, np.e)


@try_remote_functions
def log10(col: "AbstractColOrName") -> Func:
    """
    Computes the logarithm of the given value in Base 10.

    .. versionadded:: 1.4.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        AbstractCol to calculate logarithm for.

    Returns
    -------
    :class:`~osos.Col`
        logarithm of the given value in Base 10.

    Examples
    --------
    >>> df = spark.range(1)
    >>> df.select(log10(lit(100))).show()
    +----------+
    |LOG10(100)|
    +----------+
    |       2.0|
    +----------+
    """
    if isinstance(col, str):
        col = AbstractCol(col)

    return Func(log_func, col, 10)


@try_remote_functions
def log1p(col: "AbstractColOrName") -> Func:
    """
    Computes the natural logarithm of the "given value plus one".

    .. versionadded:: 1.4.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        AbstractCol to calculate natural logarithm for.

    Returns
    -------
    :class:`~osos.Col`
        natural logarithm of the "given value plus one".

    Examples
    --------
    >>> import math
    >>> df = spark.range(1)
    >>> df.select(log1p(lit(math.e))).first()
    Row(LOG1P(2.71828...)=1.31326...)

    Same as:

    >>> df.select(log(lit(math.e+1))).first()
    Row(ln(3.71828...)=1.31326...)
    """
    if isinstance(col, str):
        col = AbstractCol(col)

    return Func(log1p_func, col)


@try_remote_functions
def rint(col: "AbstractColOrName") -> Func:
    """
    Returns the double value that is closest in value to the argument and
    is equal to a mathematical integer.

    .. versionadded:: 1.4.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        target AbstractCol to compute on.

    Returns
    -------
    :class:`~osos.Col`
        the AbstractCol for computed results.

    Examples
    --------
    >>> df = spark.range(1)
    >>> df.select(rint(lit(10.6))).show()
    +----------+
    |rint(10.6)|
    +----------+
    |      11.0|
    +----------+

    >>> df.select(rint(lit(10.3))).show()
    +----------+
    |rint(10.3)|
    +----------+
    |      10.0|
    +----------+
    """
    if isinstance(col, str):
        col = AbstractCol(col)

    return Func(rint_func, col)


@try_remote_functions
def sec(col: "AbstractColOrName") -> Func:
    """
    Computes secant of the input AbstractCol.

    .. versionadded:: 3.3.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        Angle in radians

    Returns
    -------
    :class:`~osos.Col`
        Secant of the angle.

    Examples
    --------
    >>> df = spark.range(1)
    >>> df.select(sec(lit(1.5))).first()
    Row(SEC(1.5)=14.13683...)
    """
    if isinstance(col, str):
        col = AbstractCol(col)

    return Func(sec_func, col)


@try_remote_functions
def signum(col: "AbstractColOrName") -> Func:
    """
    Computes the signum of the given value.

    .. versionadded:: 1.4.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        target AbstractCol to compute on.

    Returns
    -------
    :class:`~osos.Col`
        the AbstractCol for computed results.

    Examples
    --------
    >>> df = spark.range(1)
    >>> df.select(signum(lit(-5))).show()
    +----------+
    |SIGNUM(-5)|
    +----------+
    |      -1.0|
    +----------+

    >>> df.select(signum(lit(6))).show()
    +---------+
    |SIGNUM(6)|
    +---------+
    |      1.0|
    +---------+
    """
    if isinstance(col, str):
        col = AbstractCol(col)

    return Func(signum_func, col)


@try_remote_functions
def sin(col: "AbstractColOrName") -> Func:
    """
    Computes sine of the input AbstractCol.

    .. versionadded:: 1.4.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        target AbstractCol to compute on.

    Returns
    -------
    :class:`~osos.Col`
        sine of the angle, as if computed by `java.lang.Math.sin()`

    Examples
    --------
    >>> import math
    >>> df = spark.range(1)
    >>> df.select(sin(lit(math.radians(90)))).first()
    Row(SIN(1.57079...)=1.0)
    """
    if isinstance(col, str):
        col = AbstractCol(col)

    return Func(sin_func, col)


@try_remote_functions
def sinh(col: "AbstractColOrName") -> Func:
    """
    Computes hyperbolic sine of the input AbstractCol.

    .. versionadded:: 1.4.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        hyperbolic angle.

    Returns
    -------
    :class:`~osos.Col`
        hyperbolic sine of the given value,
        as if computed by `java.lang.Math.sinh()`

    Examples
    --------
    >>> df = spark.range(1)
    >>> df.select(sinh(lit(1.1))).first()
    Row(SINH(1.1)=1.33564...)
    """
    if isinstance(col, str):
        col = AbstractCol(col)

    return Func(sinh_func, col)


@try_remote_functions
def tan(col: "AbstractColOrName") -> Func:
    """
    Computes tangent of the input AbstractCol.

    .. versionadded:: 1.4.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        angle in radians

    Returns
    -------
    :class:`~osos.Col`
        tangent of the given value, as if computed by `java.lang.Math.tan()`

    Examples
    --------
    >>> import math
    >>> df = spark.range(1)
    >>> df.select(tan(lit(math.radians(45)))).first()
    Row(TAN(0.78539...)=0.99999...)
    """
    if isinstance(col, str):
        col = AbstractCol(col)

    return Func(tan_func, col)


@try_remote_functions
def tanh(col: "AbstractColOrName") -> Func:
    """
    Computes hyperbolic tangent of the input AbstractCol.

    .. versionadded:: 1.4.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        hyperbolic angle

    Returns
    -------
    :class:`~osos.Col`
        hyperbolic tangent of the given value
        as if computed by `java.lang.Math.tanh()`

    Examples
    --------
    >>> import math
    >>> df = spark.range(1)
    >>> df.select(tanh(lit(math.radians(90)))).first()
    Row(TANH(1.57079...)=0.91715...)
    """
    if isinstance(col, str):
        col = AbstractCol(col)

    return Func(tanh_func, col)


@try_remote_functions
def toDegrees(col: "AbstractColOrName") -> Func:
    """
    .. versionadded:: 1.4.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    .. deprecated:: 2.1.0
        Use :func:`degrees` instead.
    """
    warn("Deprecated in 2.1, use degrees instead.", FutureWarning)
    return degrees_func(col)


@try_remote_functions
def toRadians(col: "AbstractColOrName") -> Func:
    """
    .. versionadded:: 1.4.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    .. deprecated:: 2.1.0
        Use :func:`radians` instead.
    """
    warn("Deprecated in 2.1, use radians instead.", FutureWarning)
    return radians_func(col)


@try_remote_functions
def bitwiseNOT(col: "AbstractColOrName") -> Func:
    """
    Computes bitwise not.

    .. versionadded:: 1.4.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    .. deprecated:: 3.2.0
        Use :func:`bitwise_not` instead.
    """
    warn("Deprecated in 3.2, use bitwise_not instead.", FutureWarning)
    return bitwise_not_func(col)


@try_remote_functions
def bitwise_not(col: "AbstractColOrName") -> Func:
    """
    Computes bitwise not.

    .. versionadded:: 3.2.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        target AbstractCol to compute on.

    Returns
    -------
    :class:`~osos.Col`
        the AbstractCol for computed results.

    Examples
    --------
    >>> df = spark.range(1)
    >>> df.select(bitwise_not(lit(0))).show()
    +---+
    | ~0|
    +---+
    | -1|
    +---+
    >>> df.select(bitwise_not(lit(1))).show()
    +---+
    | ~1|
    +---+
    | -2|
    +---+
    """
    if isinstance(col, str):
        col = AbstractCol(col)

    return Func(bitwise_not_func, col)


@try_remote_functions
def asc_nulls_first(col: "AbstractColOrName") -> Func:
    """
    Returns a sort expression based on the ascending order of the given
    AbstractCol name, and null values return before non-null values.

    .. versionadded:: 2.4.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        target AbstractCol to sort by in the ascending order.

    Returns
    -------
    :class:`~osos.Col`
        the AbstractCol specifying the order.

    Examples
    --------
    >>> df1 = spark.createDataFrame([(1, "Bob"),
    ...                              (0, None),
    ...                              (2, "Alice")], ["age", "name"])
    >>> df1.sort(asc_nulls_first(df1.name)).show()
    +---+-----+
    |age| name|
    +---+-----+
    |  0| null|
    |  2|Alice|
    |  1|  Bob|
    +---+-----+

    """
    if isinstance(col, str):
        col = AbstractCol(col)

    return Func(asc_func, col, nulls_first=True)


@try_remote_functions
def asc_nulls_last(col: "AbstractColOrName") -> Func:
    """
    Returns a sort expression based on the ascending order of the given
    AbstractCol name, and null values appear after non-null values.

    .. versionadded:: 2.4.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        target AbstractCol to sort by in the ascending order.

    Returns
    -------
    :class:`~osos.Col`
        the AbstractCol specifying the order.

    Examples
    --------
    >>> df1 = spark.createDataFrame([(0, None),
    ...                              (1, "Bob"),
    ...                              (2, "Alice")], ["age", "name"])
    >>> df1.sort(asc_nulls_last(df1.name)).show()
    +---+-----+
    |age| name|
    +---+-----+
    |  2|Alice|
    |  1|  Bob|
    |  0| null|
    +---+-----+

    """
    if isinstance(col, str):
        col = AbstractCol(col)

    return Func(asc_func, col, nulls_first=False)


@try_remote_functions
def desc_nulls_first(col: "AbstractColOrName") -> Func:
    """
    Returns a sort expression based on the descending order of the given
    AbstractCol name, and null values appear before non-null values.

    .. versionadded:: 2.4.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        target AbstractCol to sort by in the descending order.

    Returns
    -------
    :class:`~osos.Col`
        the AbstractCol specifying the order.

    Examples
    --------
    >>> df1 = spark.createDataFrame([(0, None),
    ...                              (1, "Bob"),
    ...                              (2, "Alice")], ["age", "name"])
    >>> df1.sort(desc_nulls_first(df1.name)).show()
    +---+-----+
    |age| name|
    +---+-----+
    |  0| null|
    |  1|  Bob|
    |  2|Alice|
    +---+-----+

    """
    if isinstance(col, str):
        col = AbstractCol(col)

    return Func(desc_func, col, nulls_first=True)


@try_remote_functions
def desc_nulls_last(col: "AbstractColOrName") -> Func:
    """
    Returns a sort expression based on the descending order of the given
    AbstractCol name, and null values appear after non-null values.

    .. versionadded:: 2.4.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        target AbstractCol to sort by in the descending order.

    Returns
    -------
    :class:`~osos.Col`
        the AbstractCol specifying the order.

    Examples
    --------
    >>> df1 = spark.createDataFrame([(0, None),
    ...                              (1, "Bob"),
    ...                              (2, "Alice")], ["age", "name"])
    >>> df1.sort(desc_nulls_last(df1.name)).show()
    +---+-----+
    |age| name|
    +---+-----+
    |  1|  Bob|
    |  2|Alice|
    |  0| null|
    +---+-----+

    """
    if isinstance(col, str):
        col = AbstractCol(col)

    return Func(desc_func, col, nulls_first=False)


@try_remote_functions
def stddev(col: "AbstractColOrName") -> Func:
    """
    Aggregate function: alias for stddev_samp.

    .. versionadded:: 1.6.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        target AbstractCol to compute on.

    Returns
    -------
    :class:`~osos.Col`
        standard deviation of given AbstractCol.

    Examples
    --------
    >>> df = spark.range(6)
    >>> df.select(stddev(df.id)).first()
    Row(stddev_samp(id)=1.87082...)
    """
    if isinstance(col, str):
        col = AbstractCol(col)

    return Func(stdev_func, col)


@try_remote_functions
def stddev_samp(col: "AbstractColOrName") -> Func:
    """
    Aggregate function: returns the unbiased sample standard deviation of
    the expression in a group.

    .. versionadded:: 1.6.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        target AbstractCol to compute on.

    Returns
    -------
    :class:`~osos.Col`
        standard deviation of given AbstractCol.

    Examples
    --------
    >>> df = spark.range(6)
    >>> df.select(stddev_samp(df.id)).first()
    Row(stddev_samp(id)=1.87082...)
    """
    if isinstance(col, str):
        col = AbstractCol(col)

    return Func(stdev_samp_func, col)


@try_remote_functions
def stddev_pop(col: "AbstractColOrName") -> Func:
    """
    Aggregate function: returns population standard deviation of
    the expression in a group.

    .. versionadded:: 1.6.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        target AbstractCol to compute on.

    Returns
    -------
    :class:`~osos.Col`
        standard deviation of given AbstractCol.

    Examples
    --------
    >>> df = spark.range(6)
    >>> df.select(stddev_pop(df.id)).first()
    Row(stddev_pop(id)=1.70782...)
    """
    if isinstance(col, str):
        col = AbstractCol(col)

    return Func(stdev_func, col)


@try_remote_functions
def variance(col: "AbstractColOrName") -> Func:
    """
    Aggregate function: alias for var_samp

    .. versionadded:: 1.6.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        target AbstractCol to compute on.

    Returns
    -------
    :class:`~osos.Col`
        variance of given AbstractCol.

    Examples
    --------
    >>> df = spark.range(6)
    >>> df.select(variance(df.id)).show()
    +------------+
    |var_samp(id)|
    +------------+
    |         3.5|
    +------------+
    """
    if isinstance(col, str):
        col = AbstractCol(col)

    return Func(variance_func, col)


@try_remote_functions
def var_samp(col: "AbstractColOrName") -> Func:
    """
    Aggregate function: returns the unbiased sample variance of
    the values in a group.

    .. versionadded:: 1.6.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        target AbstractCol to compute on.

    Returns
    -------
    :class:`~osos.Col`
        variance of given AbstractCol.

    Examples
    --------
    >>> df = spark.range(6)
    >>> df.select(var_samp(df.id)).show()
    +------------+
    |var_samp(id)|
    +------------+
    |         3.5|
    +------------+
    """
    if isinstance(col, str):
        col = AbstractCol(col)

    return Func(var_samp_func, col)


@try_remote_functions
def var_pop(col: "AbstractColOrName") -> Func:
    """
    Aggregate function: returns the population variance of the values in a group.

    .. versionadded:: 1.6.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        target AbstractCol to compute on.

    Returns
    -------
    :class:`~osos.Col`
        variance of given AbstractCol.

    Examples
    --------
    >>> df = spark.range(6)
    >>> df.select(var_pop(df.id)).first()
    Row(var_pop(id)=2.91666...)
    """
    if isinstance(col, str):
        col = AbstractCol(col)

    return Func(variance_func, col)


@try_remote_functions
def skewness(col: "AbstractColOrName") -> Func:
    """
    Aggregate function: returns the skewness of the values in a group.

    .. versionadded:: 1.6.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        target AbstractCol to compute on.

    Returns
    -------
    :class:`~osos.Col`
        skewness of given AbstractCol.

    Examples
    --------
    >>> df = spark.createDataFrame([[1],[1],[2]], ["c"])
    >>> df.select(skewness(df.c)).first()
    Row(skewness(c)=0.70710...)
    """
    if isinstance(col, str):
        col = AbstractCol(col)

    return Func(skewness_func, col)


@try_remote_functions
def kurtosis(col: "AbstractColOrName") -> Func:
    """
    Aggregate function: returns the kurtosis of the values in a group.

    .. versionadded:: 1.6.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        target AbstractCol to compute on.

    Returns
    -------
    :class:`~osos.Col`
        kurtosis of given AbstractCol.

    Examples
    --------
    >>> df = spark.createDataFrame([[1],[1],[2]], ["c"])
    >>> df.select(kurtosis(df.c)).show()
    +-----------+
    |kurtosis(c)|
    +-----------+
    |       -1.5|
    +-----------+
    """
    if isinstance(col, str):
        col = AbstractCol(col)

    return Func(kurtosis_func, col)


@try_remote_functions
def collect_list(col: "AbstractColOrName") -> Func:
    """
    Aggregate function: returns a list of objects with duplicates.

    .. versionadded:: 1.6.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Notes
    -----
    The function is non-deterministic because the order of collected results depends
    on the order of the rows which may be non-deterministic after a shuffle.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        target AbstractCol to compute on.

    Returns
    -------
    :class:`~osos.Col`
        list of objects with duplicates.

    Examples
    --------
    >>> df2 = spark.createDataFrame([(2,), (5,), (5,)], ('age',))
    >>> df2.agg(collect_list('age')).collect()
    [Row(collect_list(age)=[2, 5, 5])]
    """
    raise NotImplementedError


@try_remote_functions
def collect_set(col: "AbstractColOrName") -> Func:
    """
    Aggregate function: returns a set of objects with duplicate elements eliminated.

    .. versionadded:: 1.6.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Notes
    -----
    The function is non-deterministic because the order of collected results depends
    on the order of the rows which may be non-deterministic after a shuffle.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        target AbstractCol to compute on.

    Returns
    -------
    :class:`~osos.Col`
        list of objects with no duplicates.

    Examples
    --------
    >>> df2 = spark.createDataFrame([(2,), (5,), (5,)], ('age',))
    >>> df2.agg(array_sort(collect_set('age')).alias('c')).collect()
    [Row(c=[2, 5])]
    """
    raise NotImplementedError


@try_remote_functions
def degrees(col: "AbstractColOrName") -> Func:
    """
    Converts an angle measured in radians to an approximately equivalent angle
    measured in degrees.

    .. versionadded:: 2.1.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        angle in radians

    Returns
    -------
    :class:`~osos.Col`
        angle in degrees, as if computed by `java.lang.Math.toDegrees()`

    Examples
    --------
    >>> import math
    >>> df = spark.range(1)
    >>> df.select(degrees(lit(math.pi))).first()
    Row(DEGREES(3.14159...)=180.0)
    """
    if isinstance(col, str):
        col = AbstractCol(col)

    return Func(degrees_func, col)


@try_remote_functions
def radians(col: "AbstractColOrName") -> Func:
    """
    Converts an angle measured in degrees to an approximately equivalent angle
    measured in radians.

    .. versionadded:: 2.1.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        angle in degrees

    Returns
    -------
    :class:`~osos.Col`
        angle in radians, as if computed by `java.lang.Math.toRadians()`

    Examples
    --------
    >>> df = spark.range(1)
    >>> df.select(radians(lit(180))).first()
    Row(RADIANS(180)=3.14159...)
    """
    if isinstance(col, str):
        col = AbstractCol(col)

    return Func(radians_func, col)


@try_remote_functions
def atan2(
    col1: Union["AbstractColOrName", float], col2: Union["AbstractColOrName", float]
) -> Func:
    """
    .. versionadded:: 1.4.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col1 : str, :class:`~osos.Col` or float
        coordinate on y-axis
    col2 : str, :class:`~osos.Col` or float
        coordinate on x-axis

    Returns
    -------
    :class:`~osos.Col`
        the `theta` component of the point
        (`r`, `theta`)
        in polar coordinates that corresponds to the point
        (`x`, `y`) in Cartesian coordinates,
        as if computed by `java.lang.Math.atan2()`

    Examples
    --------
    >>> df = spark.range(1)
    >>> df.select(atan2(lit(1), lit(2))).first()
    Row(ATAN2(1, 2)=0.46364...)
    """
    if isinstance(col1, str):
        col = AbstractCol(col)

    if isinstance(col1, str):
        col = AbstractCol(col)

    return Func(atan2_func, col1, col2)


@try_remote_functions
def hypot(
    col1: Union["AbstractColOrName", float], col2: Union["AbstractColOrName", float]
) -> Func:
    """
    Computes ``sqrt(a^2 + b^2)`` without intermediate overflow or underflow.

    .. versionadded:: 1.4.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col1 : str, :class:`~osos.Col` or float
        a leg.
    col2 : str, :class:`~osos.Col` or float
        b leg.

    Returns
    -------
    :class:`~osos.Col`
        length of the hypotenuse.

    Examples
    --------
    >>> df = spark.range(1)
    >>> df.select(hypot(lit(1), lit(2))).first()
    Row(HYPOT(1, 2)=2.23606...)
    """
    if isinstance(col1, str):
        col = AbstractCol(col)

    if isinstance(col1, str):
        col = AbstractCol(col)

    return Func(hypot_func, col1, col2)


@try_remote_functions
def pow(
    col1: Union["AbstractColOrName", float], col2: Union["AbstractColOrName", float]
) -> Func:
    """
    Returns the value of the first argument raised to the power of the second argument.

    .. versionadded:: 1.4.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col1 : str, :class:`~osos.Col` or float
        the base number.
    col2 : str, :class:`~osos.Col` or float
        the exponent number.

    Returns
    -------
    :class:`~osos.Col`
        the base rased to the power the argument.

    Examples
    --------
    >>> df = spark.range(1)
    >>> df.select(pow(lit(3), lit(2))).first()
    Row(POWER(3, 2)=9.0)
    """
    if isinstance(col1, str):
        col = AbstractCol(col)

    if isinstance(col1, str):
        col = AbstractCol(col)

    return Func(pow_func, col1, col2)


@try_remote_functions
def pmod(
    dividend: Union["AbstractColOrName", float],
    divisor: Union["AbstractColOrName", float],
) -> Func:
    """
    Returns the positive value of dividend mod divisor.

    .. versionadded:: 3.4.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    dividend : str, :class:`~osos.Col` or float
        the AbstractCol that contains dividend, or the specified dividend value
    divisor : str, :class:`~osos.Col` or float
        the AbstractCol that contains divisor, or the specified divisor value

    Returns
    -------
    :class:`~osos.Col`
        positive value of dividend mod divisor.

    Examples
    --------
    >>> from pyspark.sql.functions import pmod
    >>> df = spark.createDataFrame([
    ...     (1.0, float('nan')), (float('nan'), 2.0), (10.0, 3.0),
    ...     (float('nan'), float('nan')), (-3.0, 4.0), (-10.0, 3.0),
    ...     (-5.0, -6.0), (7.0, -8.0), (1.0, 2.0)],
    ...     ("a", "b"))
    >>> df.select(pmod("a", "b")).show()
    +----------+
    |pmod(a, b)|
    +----------+
    |       NaN|
    |       NaN|
    |       1.0|
    |       NaN|
    |       1.0|
    |       2.0|
    |      -5.0|
    |       7.0|
    |       1.0|
    +----------+
    """
    col1, col2 = dividend, divisor
    if isinstance(col1, str):
        col = AbstractCol(col)

    if isinstance(col1, str):
        col = AbstractCol(col)

    return Func(pmod_func, col1, col2)


@try_remote_functions
def row_number() -> Func:
    """
    Window function: returns a sequential number starting at 1 within a window partition.

    .. versionadded:: 1.6.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Returns
    -------
    :class:`~osos.Col`
        the AbstractCol for calculating row numbers.

    Examples
    --------
    >>> from pyspark.sql import Window
    >>> df = spark.range(3)
    >>> w = Window.orderBy(df.id.desc())
    >>> df.withAbstractCol("desc_order", row_number().over(w)).show()
    +---+----------+
    | id|desc_order|
    +---+----------+
    |  2|         1|
    |  1|         2|
    |  0|         3|
    +---+----------+
    """
    return Func(row_number_func, AbstractIndex())


@try_remote_functions
def dense_rank() -> Func:
    """
    Window function: returns the rank of rows within a window partition, without any gaps.

    The difference between rank and dense_rank is that dense_rank leaves no gaps in ranking
    sequence when there are ties. That is, if you were ranking a competition using dense_rank
    and had three people tie for second place, you would say that all three were in second
    place and that the next person came in third. Rank would give me sequential numbers, making
    the person that came in third place (after the ties) would register as coming in fifth.

    This is equivalent to the DENSE_RANK function in SQL.

    .. versionadded:: 1.6.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Returns
    -------
    :class:`~osos.Col`
        the AbstractCol for calculating ranks.

    Examples
    --------
    >>> from pyspark.sql import Window, types
    >>> df = spark.createDataFrame([1, 1, 2, 3, 3, 4], types.IntegerType())
    >>> w = Window.orderBy("value")
    >>> df.withAbstractCol("drank", dense_rank().over(w)).show()
    +-----+-----+
    |value|drank|
    +-----+-----+
    |    1|    1|
    |    1|    1|
    |    2|    2|
    |    3|    3|
    |    3|    3|
    |    4|    4|
    +-----+-----+
    """
    return dense_rank_func()


@try_remote_functions
def rank() -> Func:
    """
    Window function: returns the rank of rows within a window partition.

    The difference between rank and dense_rank is that dense_rank leaves no gaps in ranking
    sequence when there are ties. That is, if you were ranking a competition using dense_rank
    and had three people tie for second place, you would say that all three were in second
    place and that the next person came in third. Rank would give me sequential numbers, making
    the person that came in third place (after the ties) would register as coming in fifth.

    This is equivalent to the RANK function in SQL.

    .. versionadded:: 1.6.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Returns
    -------
    :class:`~osos.Col`
        the AbstractCol for calculating ranks.

    Examples
    --------
    >>> from pyspark.sql import Window, types
    >>> df = spark.createDataFrame([1, 1, 2, 3, 3, 4], types.IntegerType())
    >>> w = Window.orderBy("value")
    >>> df.withAbstractCol("drank", rank().over(w)).show()
    +-----+-----+
    |value|drank|
    +-----+-----+
    |    1|    1|
    |    1|    1|
    |    2|    3|
    |    3|    4|
    |    3|    4|
    |    4|    6|
    +-----+-----+
    """
    return rank_func()


@try_remote_functions
def cume_dist() -> Func:
    """
    Window function: returns the cumulative distribution of values within a window partition,
    i.e. the fraction of rows that are below the current row.

    .. versionadded:: 1.6.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Returns
    -------
    :class:`~osos.Col`
        the AbstractCol for calculating cumulative distribution.

    Examples
    --------
    >>> from pyspark.sql import Window, types
    >>> df = spark.createDataFrame([1, 2, 3, 3, 4], types.IntegerType())
    >>> w = Window.orderBy("value")
    >>> df.withAbstractCol("cd", cume_dist().over(w)).show()
    +-----+---+
    |value| cd|
    +-----+---+
    |    1|0.2|
    |    2|0.4|
    |    3|0.8|
    |    3|0.8|
    |    4|1.0|
    +-----+---+
    """
    return cume_dist_func()


@try_remote_functions
def percent_rank() -> Func:
    """
    Window function: returns the relative rank (i.e. percentile) of rows within a window partition.

    .. versionadded:: 1.6.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Returns
    -------
    :class:`~osos.Col`
        the AbstractCol for calculating relative rank.

    Examples
    --------
    >>> from pyspark.sql import Window, types
    >>> df = spark.createDataFrame([1, 1, 2, 3, 3, 4], types.IntegerType())
    >>> w = Window.orderBy("value")
    >>> df.withAbstractCol("pr", percent_rank().over(w)).show()
    +-----+---+
    |value| pr|
    +-----+---+
    |    1|0.0|
    |    1|0.0|
    |    2|0.4|
    |    3|0.6|
    |    3|0.6|
    |    4|1.0|
    +-----+---+
    """
    return percent_rank_func()


@try_remote_functions
def approxCountDistinct(col: "AbstractColOrName", rsd: Optional[float] = None) -> Func:
    """
    .. versionadded:: 1.3.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    .. deprecated:: 2.1.0
        Use :func:`approx_count_distinct` instead.
    """
    warn("Deprecated in 2.1, use approx_count_distinct instead.", FutureWarning)
    return approx_count_distinct_func(col, rsd)


@try_remote_functions
def approx_count_distinct(
    col: "AbstractColOrName", rsd: Optional[float] = None
) -> Func:
    """Aggregate function: returns a new :class:`~osos.Col` for approximate distinct count
    of AbstractCol `col`.

    .. versionadded:: 2.1.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
    rsd : float, optional
        maximum relative standard deviation allowed (default = 0.05).
        For rsd < 0.01, it is more efficient to use :func:`count_distinct`

    Returns
    -------
    :class:`~osos.Col`
        the AbstractCol of computed results.

    Examples
    --------
    >>> df = spark.createDataFrame([1,2,2,3], "INT")
    >>> df.agg(approx_count_distinct("value").alias('distinct_values')).show()
    +---------------+
    |distinct_values|
    +---------------+
    |              3|
    +---------------+
    """
    if isinstance(col, str):
        col = AbstractCol(col)

    return Func(approx_count_distinct_func, col, rsd or 0.05)


@try_remote_functions
def broadcast(df: DataFrame) -> DataFrame:
    """
    Marks a DataFrame as small enough for use in broadcast joins.

    .. versionadded:: 1.6.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Returns
    -------
    :class:`~pyspark.sql.DataFrame`
        DataFrame marked as ready for broadcast join.

    Examples
    --------
    >>> from pyspark.sql import types
    >>> df = spark.createDataFrame([1, 2, 3, 3, 4], types.IntegerType())
    >>> df_small = spark.range(3)
    >>> df_b = broadcast(df_small)
    >>> df.join(df_b, df.value == df_small.id).show()
    +-----+---+
    |value| id|
    +-----+---+
    |    1|  1|
    |    2|  2|
    +-----+---+
    """

    raise NotImplementedError


@try_remote_functions
def coalesce(*cols: "AbstractColOrName") -> Func:
    """Returns the first AbstractCol that is not null.

    .. versionadded:: 1.4.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    cols : :class:`~osos.Col` or str
        list of AbstractCols to work on.

    Returns
    -------
    :class:`~osos.Col`
        value of the first AbstractCol that is not null.

    Examples
    --------
    >>> cDf = spark.createDataFrame([(None, None), (1, None), (None, 2)], ("a", "b"))
    >>> cDf.show()
    +----+----+
    |   a|   b|
    +----+----+
    |null|null|
    |   1|null|
    |null|   2|
    +----+----+

    >>> cDf.select(coalesce(cDf["a"], cDf["b"])).show()
    +--------------+
    |coalesce(a, b)|
    +--------------+
    |          null|
    |             1|
    |             2|
    +--------------+

    >>> cDf.select('*', coalesce(cDf["a"], lit(0.0))).show()
    +----+----+----------------+
    |   a|   b|coalesce(a, 0.0)|
    +----+----+----------------+
    |null|null|             0.0|
    |   1|null|             1.0|
    |null|   2|             0.0|
    +----+----+----------------+
    """
    for col in cols:
        if isinstance(col, str):
            col = AbstractCol(col)

    return Func(coalesce_func, cols)


@try_remote_functions
def corr(col1: "AbstractColOrName", col2: "AbstractColOrName") -> Func:
    """Returns a new :class:`~osos.Col` for the Pearson Correlation Coefficient for
    ``col1`` and ``col2``.

    .. versionadded:: 1.6.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col1 : :class:`~osos.Col` or str
        first AbstractCol to calculate correlation.
    col1 : :class:`~osos.Col` or str
        second AbstractCol to calculate correlation.

    Returns
    -------
    :class:`~osos.Col`
        Pearson Correlation Coefficient of these two AbstractCol values.

    Examples
    --------
    >>> a = range(20)
    >>> b = [2 * x for x in range(20)]
    >>> df = spark.createDataFrame(zip(a, b), ["a", "b"])
    >>> df.agg(corr("a", "b").alias('c')).collect()
    [Row(c=1.0)]
    """
    raise NotImplementedError


@try_remote_functions
def covar_pop(col1: "AbstractColOrName", col2: "AbstractColOrName") -> Func:
    """Returns a new :class:`~osos.Col` for the population covariance of ``col1`` and
    ``col2``.

    .. versionadded:: 2.0.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col1 : :class:`~osos.Col` or str
        first AbstractCol to calculate covariance.
    col1 : :class:`~osos.Col` or str
        second AbstractCol to calculate covariance.

    Returns
    -------
    :class:`~osos.Col`
        covariance of these two AbstractCol values.

    Examples
    --------
    >>> a = [1] * 10
    >>> b = [1] * 10
    >>> df = spark.createDataFrame(zip(a, b), ["a", "b"])
    >>> df.agg(covar_pop("a", "b").alias('c')).collect()
    [Row(c=0.0)]
    """
    raise NotImplementedError


@try_remote_functions
def covar_samp(col1: "AbstractColOrName", col2: "AbstractColOrName") -> Func:
    """Returns a new :class:`~osos.Col` for the sample covariance of ``col1`` and
    ``col2``.

    .. versionadded:: 2.0.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col1 : :class:`~osos.Col` or str
        first AbstractCol to calculate covariance.
    col1 : :class:`~osos.Col` or str
        second AbstractCol to calculate covariance.

    Returns
    -------
    :class:`~osos.Col`
        sample covariance of these two AbstractCol values.

    Examples
    --------
    >>> a = [1] * 10
    >>> b = [1] * 10
    >>> df = spark.createDataFrame(zip(a, b), ["a", "b"])
    >>> df.agg(covar_samp("a", "b").alias('c')).collect()
    [Row(c=0.0)]
    """
    raise NotImplementedError


@try_remote_functions
def countDistinct(col: "AbstractColOrName", *cols: "AbstractColOrName") -> Func:
    """Returns a new :class:`~osos.Col` for distinct count of ``col`` or ``cols``.

    An alias of :func:`count_distinct`, and it is encouraged to use :func:`count_distinct`
    directly.

    .. versionadded:: 1.3.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.
    """
    return count_distinct(col, *cols)


@try_remote_functions
def count_distinct(col: "AbstractColOrName", *cols: "AbstractColOrName") -> Func:
    """Returns a new :class:`AbstractCol` for distinct count of ``col`` or ``cols``.

    .. versionadded:: 3.2.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        first AbstractCol to compute on.
    cols : :class:`~osos.Col` or str
        other AbstractCols to compute on.

    Returns
    -------
    :class:`~osos.Col`
        distinct values of these two AbstractCol values.

    Examples
    --------
    >>> from pyspark.sql import types
    >>> df1 = spark.createDataFrame([1, 1, 3], types.IntegerType())
    >>> df2 = spark.createDataFrame([1, 2], types.IntegerType())
    >>> df1.join(df2).show()
    +-----+-----+
    |value|value|
    +-----+-----+
    |    1|    1|
    |    1|    2|
    |    1|    1|
    |    1|    2|
    |    3|    1|
    |    3|    2|
    +-----+-----+
    >>> df1.join(df2).select(count_distinct(df1.value, df2.value)).show()
    +----------------------------+
    |count(DISTINCT value, value)|
    +----------------------------+
    |                           4|
    +----------------------------+
    """
    raise NotImplemented("count_distinct", AbstractCol(col), _to_seq(cols, AbstractCol))


@try_remote_functions
def first(col: "AbstractColOrName", ignorenulls: bool = False) -> Func:
    """Aggregate function: returns the first value in a group.

    The function by default returns the first values it sees. It will return the first non-null
    value it sees when ignoreNulls is set to true. If all values are null, then null is returned.

    .. versionadded:: 1.3.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Notes
    -----
    The function is non-deterministic because its results depends on the order of the
    rows which may be non-deterministic after a shuffle.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        AbstractCol to fetch first value for.
    ignorenulls : :class:`~osos.Col` or str
        if first value is null then look for first non-null value.

    Returns
    -------
    :class:`~osos.Col`
        first value of the group.

    Examples
    --------
    >>> df = spark.createDataFrame([("Alice", 2), ("Bob", 5), ("Alice", None)], ("name", "age"))
    >>> df = df.orderBy(df.age)
    >>> df.groupby("name").agg(first("age")).orderBy("name").show()
    +-----+----------+
    | name|first(age)|
    +-----+----------+
    |Alice|      null|
    |  Bob|         5|
    +-----+----------+

    Now, to ignore any nulls we needs to set ``ignorenulls`` to `True`

    >>> df.groupby("name").agg(first("age", ignorenulls=True)).orderBy("name").show()
    +-----+----------+
    | name|first(age)|
    +-----+----------+
    |Alice|         2|
    |  Bob|         5|
    +-----+----------+
    """
    raise NotImplementedError


@try_remote_functions
def grouping(col: "AbstractColOrName") -> Func:
    """
    Aggregate function: indicates whether a specified AbstractCol in a GROUP BY list is aggregated
    or not, returns 1 for aggregated or 0 for not aggregated in the result set.

    .. versionadded:: 2.0.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        AbstractCol to check if it's aggregated.

    Returns
    -------
    :class:`~osos.Col`
        returns 1 for aggregated or 0 for not aggregated in the result set.

    Examples
    --------
    >>> df = spark.createDataFrame([("Alice", 2), ("Bob", 5)], ("name", "age"))
    >>> df.cube("name").agg(grouping("name"), sum("age")).orderBy("name").show()
    +-----+--------------+--------+
    | name|grouping(name)|sum(age)|
    +-----+--------------+--------+
    | null|             1|       7|
    |Alice|             0|       2|
    |  Bob|             0|       5|
    +-----+--------------+--------+
    """
    raise NotImplementedError


@try_remote_functions
def grouping_id(*cols: "AbstractColOrName") -> Func:
    """
    Aggregate function: returns the level of grouping, equals to

       (grouping(c1) << (n-1)) + (grouping(c2) << (n-2)) + ... + grouping(cn)

    .. versionadded:: 2.0.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Notes
    -----
    The list of AbstractCols should match with grouping AbstractCols exactly, or empty (means all
    the grouping AbstractCols).

    Parameters
    ----------
    cols : :class:`~osos.Col` or str
        AbstractCols to check for.

    Returns
    -------
    :class:`~osos.Col`
        returns level of the grouping it relates to.

    Examples
    --------
    >>> df = spark.createDataFrame([(1, "a", "a"),
    ...                             (3, "a", "a"),
    ...                             (4, "b", "c")], ["c1", "c2", "c3"])
    >>> df.cube("c2", "c3").agg(grouping_id(), sum("c1")).orderBy("c2", "c3").show()
    +----+----+-------------+-------+
    |  c2|  c3|grouping_id()|sum(c1)|
    +----+----+-------------+-------+
    |null|null|            3|      8|
    |null|   a|            2|      4|
    |null|   c|            2|      4|
    |   a|null|            1|      4|
    |   a|   a|            0|      4|
    |   b|null|            1|      4|
    |   b|   c|            0|      4|
    +----+----+-------------+-------+
    """
    raise NotImplementedError


@try_remote_functions
def input_file_name() -> Func:
    """
    Creates a string AbstractCol for the file name of the current Spark task.

    .. versionadded:: 1.6.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Returns
    -------
    :class:`~osos.Col`
        file names.

    Examples
    --------
    >>> import os
    >>> path = os.path.abspath(__file__)
    >>> df = spark.read.text(path)
    >>> df.select(input_file_name()).first()
    Row(input_file_name()='file:///...')
    """
    raise NotImplementedError


@try_remote_functions
def isnan(col: "AbstractColOrName") -> Func:
    """An expression that returns true if the AbstractCol is NaN.

    .. versionadded:: 1.6.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        target AbstractCol to compute on.

    Returns
    -------
    :class:`~osos.Col`
        True if value is NaN and False otherwise.

    Examples
    --------
    >>> df = spark.createDataFrame([(1.0, float('nan')), (float('nan'), 2.0)], ("a", "b"))
    >>> df.select("a", "b", isnan("a").alias("r1"), isnan(df.b).alias("r2")).show()
    +---+---+-----+-----+
    |  a|  b|   r1|   r2|
    +---+---+-----+-----+
    |1.0|NaN|false| true|
    |NaN|2.0| true|false|
    +---+---+-----+-----+
    """
    raise NotImplementedError


@try_remote_functions
def isnull(col: "AbstractColOrName") -> Func:
    """An expression that returns true if the AbstractCol is null.

    .. versionadded:: 1.6.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        target AbstractCol to compute on.

    Returns
    -------
    :class:`~osos.Col`
        True if value is null and False otherwise.

    Examples
    --------
    >>> df = spark.createDataFrame([(1, None), (None, 2)], ("a", "b"))
    >>> df.select("a", "b", isnull("a").alias("r1"), isnull(df.b).alias("r2")).show()
    +----+----+-----+-----+
    |   a|   b|   r1|   r2|
    +----+----+-----+-----+
    |   1|null|false| true|
    |null|   2| true|false|
    +----+----+-----+-----+
    """
    raise NotImplementedError


@try_remote_functions
def last(col: "AbstractColOrName", ignorenulls: bool = False) -> Func:
    """Aggregate function: returns the last value in a group.

    The function by default returns the last values it sees. It will return the last non-null
    value it sees when ignoreNulls is set to true. If all values are null, then null is returned.

    .. versionadded:: 1.3.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Notes
    -----
    The function is non-deterministic because its results depends on the order of the
    rows which may be non-deterministic after a shuffle.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        AbstractCol to fetch last value for.
    ignorenulls : :class:`~osos.Col` or str
        if last value is null then look for non-null value.

    Returns
    -------
    :class:`~osos.Col`
        last value of the group.

    Examples
    --------
    >>> df = spark.createDataFrame([("Alice", 2), ("Bob", 5), ("Alice", None)], ("name", "age"))
    >>> df = df.orderBy(df.age.desc())
    >>> df.groupby("name").agg(last("age")).orderBy("name").show()
    +-----+---------+
    | name|last(age)|
    +-----+---------+
    |Alice|     null|
    |  Bob|        5|
    +-----+---------+

    Now, to ignore any nulls we needs to set ``ignorenulls`` to `True`

    >>> df.groupby("name").agg(last("age", ignorenulls=True)).orderBy("name").show()
    +-----+---------+
    | name|last(age)|
    +-----+---------+
    |Alice|        2|
    |  Bob|        5|
    +-----+---------+
    """
    raise NotImplementedError


@try_remote_functions
def monotonically_increasing_id() -> Func:
    """A AbstractCol that generates monotonically increasing 64-bit integers.

    The generated ID is guaranteed to be monotonically increasing and unique, but not consecutive.
    The current implementation puts the partition ID in the upper 31 bits, and the record number
    within each partition in the lower 33 bits. The assumption is that the data frame has
    less than 1 billion partitions, and each partition has less than 8 billion records.

    .. versionadded:: 1.6.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Notes
    -----
    The function is non-deterministic because its result depends on partition IDs.

    As an example, consider a :class:`DataFrame` with two partitions, each with 3 records.
    This expression would return the following IDs:
    0, 1, 2, 8589934592 (1L << 33), 8589934593, 8589934594.

    Returns
    -------
    :class:`~osos.Col`
        last value of the group.

    Examples
    --------
    >>> df0 = sc.parallelize(range(2), 2).mapPartitions(lambda x: [(1,), (2,), (3,)]).toDF(['col1'])
    >>> df0.select(monotonically_increasing_id().alias('id')).collect()
    [Row(id=0), Row(id=1), Row(id=2), Row(id=8589934592), Row(id=8589934593), Row(id=8589934594)]
    """
    raise NotImplementedError


@try_remote_functions
def nanvl(col1: "AbstractColOrName", col2: "AbstractColOrName") -> Func:
    """Returns col1 if it is not NaN, or col2 if col1 is NaN.

    Both inputs should be floating point AbstractCols (:class:`DoubleType` or :class:`FloatType`).

    .. versionadded:: 1.6.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col1 : :class:`~osos.Col` or str
        first AbstractCol to check.
    col2 : :class:`~osos.Col` or str
        second AbstractCol to return if first is NaN.

    Returns
    -------
    :class:`~osos.Col`
        value from first AbstractCol or second if first is NaN .

    Examples
    --------
    >>> df = spark.createDataFrame([(1.0, float('nan')), (float('nan'), 2.0)], ("a", "b"))
    >>> df.select(nanvl("a", "b").alias("r1"), nanvl(df.a, df.b).alias("r2")).collect()
    [Row(r1=1.0, r2=1.0), Row(r1=2.0, r2=2.0)]
    """
    raise NotImplementedError


@try_remote_functions
def percentile_approx(
    col: "AbstractColOrName",
    percentage: Union[AbstractCol, float, List[float], Tuple[float]],
    accuracy: Union[AbstractCol, float] = 10000,
) -> Func:
    """Returns the approximate `percentile` of the numeric AbstractCol `col` which is the smallest value
    in the ordered `col` values (sorted from least to greatest) such that no more than `percentage`
    of `col` values is less than the value or equal to that value.


    .. versionadded:: 3.1.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        input AbstractCol.
    percentage : :class:`~osos.Col`, float, list of floats or tuple of floats
        percentage in decimal (must be between 0.0 and 1.0).
        When percentage is an array, each value of the percentage array must be between 0.0 and 1.0.
        In this case, returns the approximate percentile array of AbstractCol col
        at the given percentage array.
    accuracy : :class:`~osos.Col` or float
        is a positive numeric literal which controls approximation accuracy
        at the cost of memory. Higher value of accuracy yields better accuracy,
        1.0/accuracy is the relative error of the approximation. (default: 10000).

    Returns
    -------
    :class:`~osos.Col`
        approximate `percentile` of the numeric AbstractCol.

    Examples
    --------
    >>> key = (col("id") % 3).alias("key")
    >>> value = (randn(42) + key * 10).alias("value")
    >>> df = spark.range(0, 1000, 1, 1).select(key, value)
    >>> df.select(
    ...     percentile_approx("value", [0.25, 0.5, 0.75], 1000000).alias("quantiles")
    ... ).printSchema()
    root
     |-- quantiles: array (nullable = true)
     |    |-- element: double (containsNull = false)

    >>> df.groupBy("key").agg(
    ...     percentile_approx("value", 0.5, lit(1000000)).alias("median")
    ... ).printSchema()
    root
     |-- key: long (nullable = true)
     |-- median: double (nullable = true)
    """

    if isinstance(percentage, AbstractCol):
        # Already a AbstractCol
        percentage = AbstractCol(percentage)
    else:
        # Probably scalar
        percentage = AbstractLit(percentage)

    accuracy = (
        AbstractCol(accuracy)
        if isinstance(accuracy, AbstractCol)
        else AbstractLit(accuracy)
    )

    raise NotImplementedError


@try_remote_functions
def rand(seed: Optional[int] = None) -> Func:
    """Generates a random AbstractCol with independent and identically distributed (i.i.d.) samples
    uniformly distributed in [0.0, 1.0).

    .. versionadded:: 1.4.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Notes
    -----
    The function is non-deterministic in general case.

    Parameters
    ----------
    seed : int (default: None)
        seed value for random generator.

    Returns
    -------
    :class:`~osos.Col`
        random values.

    Examples
    --------
    >>> df = spark.range(2)
    >>> df.withAbstractCol('rand', rand(seed=42) * 3).show() # doctest: +SKIP
    +---+------------------+
    | id|              rand|
    +---+------------------+
    |  0|1.4385751892400076|
    |  1|1.7082186019706387|
    +---+------------------+
    """
    if seed is not None:
        raise NotImplementedError
    else:
        raise NotImplementedError


@try_remote_functions
def randn(seed: Optional[int] = None) -> Func:
    """Generates a AbstractCol with independent and identically distributed (i.i.d.) samples from
    the standard normal distribution.

    .. versionadded:: 1.4.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Notes
    -----
    The function is non-deterministic in general case.

    Parameters
    ----------
    seed : int (default: None)
        seed value for random generator.

    Returns
    -------
    :class:`~osos.Col`
        random values.

    Examples
    --------
    >>> df = spark.range(2)
    >>> df.withAbstractCol('randn', randn(seed=42)).show() # doctest: +SKIP
    +---+--------------------+
    | id|               randn|
    +---+--------------------+
    |  0|-0.04167221574820542|
    |  1| 0.15241403986452778|
    +---+--------------------+
    """
    if seed is not None:
        raise NotImplementedError
    else:
        raise NotImplementedError


@try_remote_functions
def round(col: "AbstractColOrName", scale: int = 0) -> Func:
    """
    Round the given value to `scale` decimal places using HALF_UP rounding mode if `scale` >= 0
    or at integral part when `scale` < 0.

    .. versionadded:: 1.5.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        input AbstractCol to round.
    scale : int optional default 0
        scale value.

    Returns
    -------
    :class:`~osos.Col`
        rounded values.

    Examples
    --------
    >>> spark.createDataFrame([(2.5,)], ['a']).select(round('a', 0).alias('r')).collect()
    [Row(r=3.0)]
    """
    raise NotImplementedError


@try_remote_functions
def bround(col: "AbstractColOrName", scale: int = 0) -> Func:
    """
    Round the given value to `scale` decimal places using HALF_EVEN rounding mode if `scale` >= 0
    or at integral part when `scale` < 0.

    .. versionadded:: 2.0.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        input AbstractCol to round.
    scale : int optional default 0
        scale value.

    Returns
    -------
    :class:`~osos.Col`
        rounded values.

    Examples
    --------
    >>> spark.createDataFrame([(2.5,)], ['a']).select(bround('a', 0).alias('r')).collect()
    [Row(r=2.0)]
    """
    raise NotImplementedError


@try_remote_functions
def shiftLeft(col: "AbstractColOrName", numBits: int) -> Func:
    """Shift the given value numBits left.

    .. versionadded:: 1.5.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    .. deprecated:: 3.2.0
        Use :func:`shiftleft` instead.
    """
    warn("Deprecated in 3.2, use shiftleft instead.", FutureWarning)
    return shiftleft(col, numBits)


@try_remote_functions
def shiftleft(col: "AbstractColOrName", numBits: int) -> Func:
    """Shift the given value numBits left.

    .. versionadded:: 3.2.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        input AbstractCol of values to shift.
    numBits : int
        number of bits to shift.

    Returns
    -------
    :class:`~osos.Col`
        shifted value.

    Examples
    --------
    >>> spark.createDataFrame([(21,)], ['a']).select(shiftleft('a', 1).alias('r')).collect()
    [Row(r=42)]
    """
    raise NotImplementedError


@try_remote_functions
def shiftRight(col: "AbstractColOrName", numBits: int) -> Func:
    """(Signed) shift the given value numBits right.

    .. versionadded:: 1.5.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    .. deprecated:: 3.2.0
        Use :func:`shiftright` instead.
    """
    warn("Deprecated in 3.2, use shiftright instead.", FutureWarning)
    return shiftright(col, numBits)


@try_remote_functions
def shiftright(col: "AbstractColOrName", numBits: int) -> Func:
    """(Signed) shift the given value numBits right.

    .. versionadded:: 3.2.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        input AbstractCol of values to shift.
    numBits : int
        number of bits to shift.

    Returns
    -------
    :class:`~osos.Col`
        shifted values.

    Examples
    --------
    >>> spark.createDataFrame([(42,)], ['a']).select(shiftright('a', 1).alias('r')).collect()
    [Row(r=21)]
    """
    raise NotImplementedError


@try_remote_functions
def shiftRightUnsigned(col: "AbstractColOrName", numBits: int) -> Func:
    """Unsigned shift the given value numBits right.

    .. versionadded:: 1.5.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    .. deprecated:: 3.2.0
        Use :func:`shiftrightunsigned` instead.
    """
    warn("Deprecated in 3.2, use shiftrightunsigned instead.", FutureWarning)
    return shiftrightunsigned(col, numBits)


@try_remote_functions
def shiftrightunsigned(col: "AbstractColOrName", numBits: int) -> Func:
    """Unsigned shift the given value numBits right.

    .. versionadded:: 3.2.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        input AbstractCol of values to shift.
    numBits : int
        number of bits to shift.

    Returns
    -------
    :class:`~osos.Col`
        shifted value.

    Examples
    --------
    >>> df = spark.createDataFrame([(-42,)], ['a'])
    >>> df.select(shiftrightunsigned('a', 1).alias('r')).collect()
    [Row(r=9223372036854775787)]
    """
    raise NotImplementedError


@try_remote_functions
def spark_partition_id() -> Func:
    """A AbstractCol for partition ID.

    .. versionadded:: 1.6.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Notes
    -----
    This is non deterministic because it depends on data partitioning and task scheduling.

    Returns
    -------
    :class:`~osos.Col`
        partition id the record belongs to.

    Examples
    --------
    >>> df = spark.range(2)
    >>> df.repartition(1).select(spark_partition_id().alias("pid")).collect()
    [Row(pid=0), Row(pid=0)]
    """
    raise NotImplementedError


@try_remote_functions
def expr(str: str) -> Func:
    """Parses the expression string into the AbstractCol that it represents

    .. versionadded:: 1.5.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    str : str
        expression defined in string.

    Returns
    -------
    :class:`~osos.Col`
        AbstractCol representing the expression.

    Examples
    --------
    >>> df = spark.createDataFrame([["Alice"], ["Bob"]], ["name"])
    >>> df.select("name", expr("length(name)")).show()
    +-----+------------+
    | name|length(name)|
    +-----+------------+
    |Alice|           5|
    |  Bob|           3|
    +-----+------------+
    """
    raise NotImplementedError


@overload
def struct(*cols: "AbstractColOrName") -> Func:
    ...


@overload
def struct(
    __cols: Union[List["AbstractColOrName"], Tuple["AbstractColOrName", ...]]
) -> Func:
    ...


@try_remote_functions
def struct(
    *cols: Union[
        "AbstractColOrName",
        Union[List["AbstractColOrName"], Tuple["AbstractColOrName", ...]],
    ]
) -> Func:
    """Creates a new struct AbstractCol.

    .. versionadded:: 1.4.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    cols : list, set, str or :class:`~osos.Col`
        AbstractCol names or :class:`~osos.Col`\\s to contain in the output struct.

    Returns
    -------
    :class:`~osos.Col`
        a struct type AbstractCol of given AbstractCols.

    Examples
    --------
    >>> df = spark.createDataFrame([("Alice", 2), ("Bob", 5)], ("name", "age"))
    >>> df.select(struct('age', 'name').alias("struct")).collect()
    [Row(struct=Row(age=2, name='Alice')), Row(struct=Row(age=5, name='Bob'))]
    >>> df.select(struct([df.age, df.name]).alias("struct")).collect()
    [Row(struct=Row(age=2, name='Alice')), Row(struct=Row(age=5, name='Bob'))]
    """
    if len(cols) == 1 and isinstance(cols[0], (list, set)):
        cols = cols[0]  # type: ignore[assignment]
    raise NotImplementedError


@try_remote_functions
def greatest(*cols: "AbstractColOrName") -> Func:
    """
    Returns the greatest value of the list of AbstractCol names, skipping null values.
    This function takes at least 2 parameters. It will return null if all parameters are null.

    .. versionadded:: 1.5.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        AbstractCols to check for gratest value.

    Returns
    -------
    :class:`~osos.Col`
        gratest value.

    Examples
    --------
    >>> df = spark.createDataFrame([(1, 4, 3)], ['a', 'b', 'c'])
    >>> df.select(greatest(df.a, df.b, df.c).alias("greatest")).collect()
    [Row(greatest=4)]
    """
    if len(cols) < 2:
        raise OsosValueError(
            error_class="WRONG_NUM_AbstractColS",
            message_parameters={"func_name": "greatest", "num_cols": "2"},
        )
    raise NotImplementedError


@try_remote_functions
def least(*cols: "AbstractColOrName") -> Func:
    """
    Returns the least value of the list of AbstractCol names, skipping null values.
    This function takes at least 2 parameters. It will return null if all parameters are null.

    .. versionadded:: 1.5.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    cols : :class:`~osos.Col` or str
        AbstractCol names or AbstractCols to be compared

    Returns
    -------
    :class:`~osos.Col`
        least value.

    Examples
    --------
    >>> df = spark.createDataFrame([(1, 4, 3)], ['a', 'b', 'c'])
    >>> df.select(least(df.a, df.b, df.c).alias("least")).collect()
    [Row(least=1)]
    """
    if len(cols) < 2:
        raise ososValueError(
            error_class="WRONG_NUM_AbstractColS",
            message_parameters={"func_name": "least", "num_cols": "2"},
        )
    raise NotImplementedError


@try_remote_functions
def when(condition: AbstractCol, value: Any) -> Func:
    """Evaluates a list of conditions and returns one of multiple possible result expressions.
    If :func:`osos.Col.otherwise` is not invoked, None is returned for unmatched
    conditions.

    .. versionadded:: 1.4.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    condition : :class:`~osos.Col`
        a boolean :class:`~osos.Col` expression.
    value :
        a literal value, or a :class:`~osos.Col` expression.

    Returns
    -------
    :class:`~osos.Col`
        AbstractCol representing when expression.

    Examples
    --------
    >>> df = spark.range(3)
    >>> df.select(when(df['id'] == 2, 3).otherwise(4).alias("age")).show()
    +---+
    |age|
    +---+
    |  4|
    |  4|
    |  3|
    +---+

    >>> df.select(when(df.id == 2, df.id + 1).alias("age")).show()
    +----+
    | age|
    +----+
    |null|
    |null|
    |   3|
    +----+
    """
    # Explicitly not using AbstractColOrName type here to make reading condition less opaque
    if not isinstance(condition, AbstractCol):
        raise ososTypeError(
            error_class="NOT_AbstractCol",
            message_parameters={
                "arg_name": "condition",
                "arg_type": type(condition).__name__,
            },
        )
    v = value._jc if isinstance(value, AbstractCol) else value

    raise NotImplementedError


@overload  # type: ignore[no-redef]
def log(arg1: "AbstractColOrName") -> Func:
    ...


@overload
def log(arg1: float, arg2: "AbstractColOrName") -> Func:
    ...


@try_remote_functions
def log(
    arg1: Union["AbstractColOrName", float], arg2: Optional["AbstractColOrName"] = None
) -> Func:
    """Returns the first argument-based logarithm of the second argument.

    If there is only one argument, then this takes the natural logarithm of the argument.

    .. versionadded:: 1.5.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    arg1 : :class:`~osos.Col`, str or float
        base number or actual number (in this case base is `e`)
    arg2 : :class:`~osos.Col`, str or float
        number to calculate logariphm for.

    Returns
    -------
    :class:`~osos.Col`
        logariphm of given value.

    Examples
    --------
    >>> df = spark.createDataFrame([10, 100, 1000], "INT")
    >>> df.select(log(10.0, df.value).alias('ten')).show() # doctest: +SKIP
    +---+
    |ten|
    +---+
    |1.0|
    |2.0|
    |3.0|
    +---+

    And Natural logarithm

    >>> df.select(log(df.value)).show() # doctest: +SKIP
    +-----------------+
    |        ln(value)|
    +-----------------+
    |2.302585092994046|
    |4.605170185988092|
    |4.605170185988092|
    +-----------------+
    """
    if arg2 is None:
        raise NotImplementedError
    else:
        raise NotImplementedError


@try_remote_functions
def log2(col: "AbstractColOrName") -> Func:
    """Returns the base-2 logarithm of the argument.

    .. versionadded:: 1.5.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        a AbstractCol to calculate logariphm for.

    Returns
    -------
    :class:`~osos.Col`
        logariphm of given value.

    Examples
    --------
    >>> df = spark.createDataFrame([(4,)], ['a'])
    >>> df.select(log2('a').alias('log2')).show()
    +----+
    |log2|
    +----+
    | 2.0|
    +----+
    """
    raise NotImplementedError


@try_remote_functions
def conv(col: "AbstractColOrName", fromBase: int, toBase: int) -> Func:
    """
    Convert a number in a string AbstractCol from one base to another.

    .. versionadded:: 1.5.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        a AbstractCol to convert base for.
    fromBase: int
        from base number.
    toBase: int
        to base number.

    Returns
    -------
    :class:`~osos.Col`
        logariphm of given value.

    Examples
    --------
    >>> df = spark.createDataFrame([("010101",)], ['n'])
    >>> df.select(conv(df.n, 2, 16).alias('hex')).collect()
    [Row(hex='15')]
    """
    raise NotImplementedError


@try_remote_functions
def factorial(col: "AbstractColOrName") -> Func:
    """
    Computes the factorial of the given value.

    .. versionadded:: 1.5.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        a AbstractCol to calculate factorial for.

    Returns
    -------
    :class:`~osos.Col`
        factorial of given value.

    Examples
    --------
    >>> df = spark.createDataFrame([(5,)], ['n'])
    >>> df.select(factorial(df.n).alias('f')).collect()
    [Row(f=120)]
    """
    raise NotImplementedError


# ---------------  Window functions ------------------------


@try_remote_functions
def lag(
    col: "AbstractColOrName", offset: int = 1, default: Optional[Any] = None
) -> Func:
    """
    Window function: returns the value that is `offset` rows before the current row, and
    `default` if there is less than `offset` rows before the current row. For example,
    an `offset` of one will return the previous row at any given point in the window partition.

    This is equivalent to the LAG function in SQL.

    .. versionadded:: 1.4.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        name of AbstractCol or expression
    offset : int, optional default 1
        number of row to extend
    default : optional
        default value

    Returns
    -------
    :class:`~osos.Col`
        value before current row based on `offset`.

    Examples
    --------
    >>> from pyspark.sql import Window
    >>> df = spark.createDataFrame([("a", 1),
    ...                             ("a", 2),
    ...                             ("a", 3),
    ...                             ("b", 8),
    ...                             ("b", 2)], ["c1", "c2"])
    >>> df.show()
    +---+---+
    | c1| c2|
    +---+---+
    |  a|  1|
    |  a|  2|
    |  a|  3|
    |  b|  8|
    |  b|  2|
    +---+---+
    >>> w = Window.partitionBy("c1").orderBy("c2")
    >>> df.withAbstractCol("previos_value", lag("c2").over(w)).show()
    +---+---+-------------+
    | c1| c2|previos_value|
    +---+---+-------------+
    |  a|  1|         null|
    |  a|  2|            1|
    |  a|  3|            2|
    |  b|  2|         null|
    |  b|  8|            2|
    +---+---+-------------+
    >>> df.withAbstractCol("previos_value", lag("c2", 1, 0).over(w)).show()
    +---+---+-------------+
    | c1| c2|previos_value|
    +---+---+-------------+
    |  a|  1|            0|
    |  a|  2|            1|
    |  a|  3|            2|
    |  b|  2|            0|
    |  b|  8|            2|
    +---+---+-------------+
    >>> df.withAbstractCol("previos_value", lag("c2", 2, -1).over(w)).show()
    +---+---+-------------+
    | c1| c2|previos_value|
    +---+---+-------------+
    |  a|  1|           -1|
    |  a|  2|           -1|
    |  a|  3|            1|
    |  b|  2|           -1|
    |  b|  8|           -1|
    +---+---+-------------+
    """
    raise NotImplementedError


@try_remote_functions
def lead(
    col: "AbstractColOrName", offset: int = 1, default: Optional[Any] = None
) -> Func:
    """
    Window function: returns the value that is `offset` rows after the current row, and
    `default` if there is less than `offset` rows after the current row. For example,
    an `offset` of one will return the next row at any given point in the window partition.

    This is equivalent to the LEAD function in SQL.

    .. versionadded:: 1.4.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        name of AbstractCol or expression
    offset : int, optional default 1
        number of row to extend
    default : optional
        default value

    Returns
    -------
    :class:`~osos.Col`
        value after current row based on `offset`.

    Examples
    --------
    >>> from pyspark.sql import Window
    >>> df = spark.createDataFrame([("a", 1),
    ...                             ("a", 2),
    ...                             ("a", 3),
    ...                             ("b", 8),
    ...                             ("b", 2)], ["c1", "c2"])
    >>> df.show()
    +---+---+
    | c1| c2|
    +---+---+
    |  a|  1|
    |  a|  2|
    |  a|  3|
    |  b|  8|
    |  b|  2|
    +---+---+
    >>> w = Window.partitionBy("c1").orderBy("c2")
    >>> df.withAbstractCol("next_value", lead("c2").over(w)).show()
    +---+---+----------+
    | c1| c2|next_value|
    +---+---+----------+
    |  a|  1|         2|
    |  a|  2|         3|
    |  a|  3|      null|
    |  b|  2|         8|
    |  b|  8|      null|
    +---+---+----------+
    >>> df.withAbstractCol("next_value", lead("c2", 1, 0).over(w)).show()
    +---+---+----------+
    | c1| c2|next_value|
    +---+---+----------+
    |  a|  1|         2|
    |  a|  2|         3|
    |  a|  3|         0|
    |  b|  2|         8|
    |  b|  8|         0|
    +---+---+----------+
    >>> df.withAbstractCol("next_value", lead("c2", 2, -1).over(w)).show()
    +---+---+----------+
    | c1| c2|next_value|
    +---+---+----------+
    |  a|  1|         3|
    |  a|  2|        -1|
    |  a|  3|        -1|
    |  b|  2|        -1|
    |  b|  8|        -1|
    +---+---+----------+
    """
    raise NotImplementedError


@try_remote_functions
def nth_value(
    col: "AbstractColOrName", offset: int, ignoreNulls: Optional[bool] = False
) -> Func:
    """
    Window function: returns the value that is the `offset`\\th row of the window frame
    (counting from 1), and `null` if the size of window frame is less than `offset` rows.

    It will return the `offset`\\th non-null value it sees when `ignoreNulls` is set to
    true. If all values are null, then null is returned.

    This is equivalent to the nth_value function in SQL.

    .. versionadded:: 3.1.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        name of AbstractCol or expression
    offset : int
        number of row to use as the value
    ignoreNulls : bool, optional
        indicates the Nth value should skip null in the
        determination of which row to use

    Returns
    -------
    :class:`~osos.Col`
        value of nth row.

    Examples
    --------
    >>> from pyspark.sql import Window
    >>> df = spark.createDataFrame([("a", 1),
    ...                             ("a", 2),
    ...                             ("a", 3),
    ...                             ("b", 8),
    ...                             ("b", 2)], ["c1", "c2"])
    >>> df.show()
    +---+---+
    | c1| c2|
    +---+---+
    |  a|  1|
    |  a|  2|
    |  a|  3|
    |  b|  8|
    |  b|  2|
    +---+---+
    >>> w = Window.partitionBy("c1").orderBy("c2")
    >>> df.withAbstractCol("nth_value", nth_value("c2", 1).over(w)).show()
    +---+---+---------+
    | c1| c2|nth_value|
    +---+---+---------+
    |  a|  1|        1|
    |  a|  2|        1|
    |  a|  3|        1|
    |  b|  2|        2|
    |  b|  8|        2|
    +---+---+---------+
    >>> df.withAbstractCol("nth_value", nth_value("c2", 2).over(w)).show()
    +---+---+---------+
    | c1| c2|nth_value|
    +---+---+---------+
    |  a|  1|     null|
    |  a|  2|        2|
    |  a|  3|        2|
    |  b|  2|     null|
    |  b|  8|        8|
    +---+---+---------+
    """
    raise NotImplementedError


@try_remote_functions
def ntile(n: int) -> Func:
    """
    Window function: returns the ntile group id (from 1 to `n` inclusive)
    in an ordered window partition. For example, if `n` is 4, the first
    quarter of the rows will get value 1, the second quarter will get 2,
    the third quarter will get 3, and the last quarter will get 4.

    This is equivalent to the NTILE function in SQL.

    .. versionadded:: 1.4.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    n : int
        an integer

    Returns
    -------
    :class:`~osos.Col`
        portioned group id.

    Examples
    --------
    >>> from pyspark.sql import Window
    >>> df = spark.createDataFrame([("a", 1),
    ...                             ("a", 2),
    ...                             ("a", 3),
    ...                             ("b", 8),
    ...                             ("b", 2)], ["c1", "c2"])
    >>> df.show()
    +---+---+
    | c1| c2|
    +---+---+
    |  a|  1|
    |  a|  2|
    |  a|  3|
    |  b|  8|
    |  b|  2|
    +---+---+
    >>> w = Window.partitionBy("c1").orderBy("c2")
    >>> df.withAbstractCol("ntile", ntile(2).over(w)).show()
    +---+---+-----+
    | c1| c2|ntile|
    +---+---+-----+
    |  a|  1|    1|
    |  a|  2|    1|
    |  a|  3|    2|
    |  b|  2|    1|
    |  b|  8|    2|
    +---+---+-----+
    """
    raise NotImplementedError


# ---------------------- Date/Timestamp functions ------------------------------


@try_remote_functions
def current_date() -> Func:
    """
    Returns the current date at the start of query evaluation as a :class:`DateType` AbstractCol.
    All calls of current_date within the same query return the same value.

    .. versionadded:: 1.5.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Returns
    -------
    :class:`~osos.Col`
        current date.

    Examples
    --------
    >>> df = spark.range(1)
    >>> df.select(current_date()).show() # doctest: +SKIP
    +--------------+
    |current_date()|
    +--------------+
    |    2022-08-26|
    +--------------+
    """
    raise NotImplementedError


@try_remote_functions
def current_timestamp() -> Func:
    """
    Returns the current timestamp at the start of query evaluation as a :class:`TimestampType`
    AbstractCol. All calls of current_timestamp within the same query return the same value.

    .. versionadded:: 1.5.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Returns
    -------
    :class:`~osos.Col`
        current date and time.

    Examples
    --------
    >>> df = spark.range(1)
    >>> df.select(current_timestamp()).show(truncate=False) # doctest: +SKIP
    +-----------------------+
    |current_timestamp()    |
    +-----------------------+
    |2022-08-26 21:23:22.716|
    +-----------------------+
    """
    raise NotImplementedError


@try_remote_functions
def localtimestamp() -> Func:
    """
    Returns the current timestamp without time zone at the start of query evaluation
    as a timestamp without time zone AbstractCol. All calls of localtimestamp within the
    same query return the same value.

    .. versionadded:: 3.4.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Returns
    -------
    :class:`~osos.Col`
        current local date and time.

    Examples
    --------
    >>> df = spark.range(1)
    >>> df.select(localtimestamp()).show(truncate=False) # doctest: +SKIP
    +-----------------------+
    |localtimestamp()       |
    +-----------------------+
    |2022-08-26 21:28:34.639|
    +-----------------------+
    """
    raise NotImplementedError


@try_remote_functions
def date_format(date: "AbstractColOrName", format: str) -> Func:
    """
    Converts a date/timestamp/string to a value of string in the format specified by the date
    format given by the second argument.

    A pattern could be for instance `dd.MM.yyyy` and could return a string like '18.03.1993'. All
    pattern letters of `datetime pattern`_. can be used.

    .. _datetime pattern: https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html

    .. versionadded:: 1.5.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Notes
    -----
    Whenever possible, use specialized functions like `year`.

    Parameters
    ----------
    date : :class:`~osos.Col` or str
        input AbstractCol of values to format.
    format: str
        format to use to represent datetime values.

    Returns
    -------
    :class:`~osos.Col`
        string value representing formatted datetime.

    Examples
    --------
    >>> df = spark.createDataFrame([('2015-04-08',)], ['dt'])
    >>> df.select(date_format('dt', 'MM/dd/yyy').alias('date')).collect()
    [Row(date='04/08/2015')]
    """
    raise NotImplementedError


@try_remote_functions
def year(col: "AbstractColOrName") -> Func:
    """
    Extract the year of a given date/timestamp as integer.

    .. versionadded:: 1.5.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        target date/timestamp AbstractCol to work on.

    Returns
    -------
    :class:`~osos.Col`
        year part of the date/timestamp as integer.

    Examples
    --------
    >>> df = spark.createDataFrame([('2015-04-08',)], ['dt'])
    >>> df.select(year('dt').alias('year')).collect()
    [Row(year=2015)]
    """
    raise NotImplementedError


@try_remote_functions
def quarter(col: "AbstractColOrName") -> Func:
    """
    Extract the quarter of a given date/timestamp as integer.

    .. versionadded:: 1.5.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        target date/timestamp AbstractCol to work on.

    Returns
    -------
    :class:`~osos.Col`
        quarter of the date/timestamp as integer.

    Examples
    --------
    >>> df = spark.createDataFrame([('2015-04-08',)], ['dt'])
    >>> df.select(quarter('dt').alias('quarter')).collect()
    [Row(quarter=2)]
    """
    raise NotImplementedError


@try_remote_functions
def month(col: "AbstractColOrName") -> Func:
    """
    Extract the month of a given date/timestamp as integer.

    .. versionadded:: 1.5.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        target date/timestamp AbstractCol to work on.

    Returns
    -------
    :class:`~osos.Col`
        month part of the date/timestamp as integer.

    Examples
    --------
    >>> df = spark.createDataFrame([('2015-04-08',)], ['dt'])
    >>> df.select(month('dt').alias('month')).collect()
    [Row(month=4)]
    """
    raise NotImplementedError


@try_remote_functions
def dayofweek(col: "AbstractColOrName") -> Func:
    """
    Extract the day of the week of a given date/timestamp as integer.
    Ranges from 1 for a Sunday through to 7 for a Saturday

    .. versionadded:: 2.3.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        target date/timestamp AbstractCol to work on.

    Returns
    -------
    :class:`~osos.Col`
        day of the week for given date/timestamp as integer.

    Examples
    --------
    >>> df = spark.createDataFrame([('2015-04-08',)], ['dt'])
    >>> df.select(dayofweek('dt').alias('day')).collect()
    [Row(day=4)]
    """
    raise NotImplementedError


@try_remote_functions
def dayofmonth(col: "AbstractColOrName") -> Func:
    """
    Extract the day of the month of a given date/timestamp as integer.

    .. versionadded:: 1.5.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        target date/timestamp AbstractCol to work on.

    Returns
    -------
    :class:`~osos.Col`
        day of the month for given date/timestamp as integer.

    Examples
    --------
    >>> df = spark.createDataFrame([('2015-04-08',)], ['dt'])
    >>> df.select(dayofmonth('dt').alias('day')).collect()
    [Row(day=8)]
    """
    raise NotImplementedError


@try_remote_functions
def dayofyear(col: "AbstractColOrName") -> Func:
    """
    Extract the day of the year of a given date/timestamp as integer.

    .. versionadded:: 1.5.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        target date/timestamp AbstractCol to work on.

    Returns
    -------
    :class:`~osos.Col`
        day of the year for given date/timestamp as integer.

    Examples
    --------
    >>> df = spark.createDataFrame([('2015-04-08',)], ['dt'])
    >>> df.select(dayofyear('dt').alias('day')).collect()
    [Row(day=98)]
    """
    raise NotImplementedError


@try_remote_functions
def hour(col: "AbstractColOrName") -> Func:
    """
    Extract the hours of a given timestamp as integer.

    .. versionadded:: 1.5.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        target date/timestamp AbstractCol to work on.

    Returns
    -------
    :class:`~osos.Col`
        hour part of the timestamp as integer.

    Examples
    --------
    >>> import datetime
    >>> df = spark.createDataFrame([(datetime.datetime(2015, 4, 8, 13, 8, 15),)], ['ts'])
    >>> df.select(hour('ts').alias('hour')).collect()
    [Row(hour=13)]
    """
    raise NotImplementedError


@try_remote_functions
def minute(col: "AbstractColOrName") -> Func:
    """
    Extract the minutes of a given timestamp as integer.

    .. versionadded:: 1.5.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        target date/timestamp AbstractCol to work on.

    Returns
    -------
    :class:`~osos.Col`
        minutes part of the timestamp as integer.

    Examples
    --------
    >>> import datetime
    >>> df = spark.createDataFrame([(datetime.datetime(2015, 4, 8, 13, 8, 15),)], ['ts'])
    >>> df.select(minute('ts').alias('minute')).collect()
    [Row(minute=8)]
    """
    raise NotImplementedError


@try_remote_functions
def second(col: "AbstractColOrName") -> Func:
    """
    Extract the seconds of a given date as integer.

    .. versionadded:: 1.5.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        target date/timestamp AbstractCol to work on.

    Returns
    -------
    :class:`~osos.Col`
        `seconds` part of the timestamp as integer.

    Examples
    --------
    >>> import datetime
    >>> df = spark.createDataFrame([(datetime.datetime(2015, 4, 8, 13, 8, 15),)], ['ts'])
    >>> df.select(second('ts').alias('second')).collect()
    [Row(second=15)]
    """
    raise NotImplementedError


@try_remote_functions
def weekofyear(col: "AbstractColOrName") -> Func:
    """
    Extract the week number of a given date as integer.
    A week is considered to start on a Monday and week 1 is the first week with more than 3 days,
    as defined by ISO 8601

    .. versionadded:: 1.5.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        target timestamp AbstractCol to work on.

    Returns
    -------
    :class:`~osos.Col`
        `week` of the year for given date as integer.

    Examples
    --------
    >>> df = spark.createDataFrame([('2015-04-08',)], ['dt'])
    >>> df.select(weekofyear(df.dt).alias('week')).collect()
    [Row(week=15)]
    """
    raise NotImplementedError


@try_remote_functions
def make_date(
    year: "AbstractColOrName", month: "AbstractColOrName", day: "AbstractColOrName"
) -> Func:
    """
    Returns a AbstractCol with a date built from the year, month and day AbstractCols.

    .. versionadded:: 3.3.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    year : :class:`~osos.Col` or str
        The year to build the date
    month : :class:`~osos.Col` or str
        The month to build the date
    day : :class:`~osos.Col` or str
        The day to build the date

    Returns
    -------
    :class:`~osos.Col`
        a date built from given parts.

    Examples
    --------
    >>> df = spark.createDataFrame([(2020, 6, 26)], ['Y', 'M', 'D'])
    >>> df.select(make_date(df.Y, df.M, df.D).alias("datefield")).collect()
    [Row(datefield=datetime.date(2020, 6, 26))]
    """
    raise NotImplementedError


@try_remote_functions
def date_add(start: "AbstractColOrName", days: Union["AbstractColOrName", int]) -> Func:
    """
    Returns the date that is `days` days after `start`. If `days` is a negative value
    then these amount of days will be deducted from `start`.

    .. versionadded:: 1.5.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    start : :class:`~osos.Col` or str
        date AbstractCol to work on.
    days : :class:`~osos.Col` or str or int
        how many days after the given date to calculate.
        Accepts negative value as well to calculate backwards in time.

    Returns
    -------
    :class:`~osos.Col`
        a date after/before given number of days.

    Examples
    --------
    >>> df = spark.createDataFrame([('2015-04-08', 2,)], ['dt', 'add'])
    >>> df.select(date_add(df.dt, 1).alias('next_date')).collect()
    [Row(next_date=datetime.date(2015, 4, 9))]
    >>> df.select(date_add(df.dt, df.add.cast('integer')).alias('next_date')).collect()
    [Row(next_date=datetime.date(2015, 4, 10))]
    >>> df.select(date_add('dt', -1).alias('prev_date')).collect()
    [Row(prev_date=datetime.date(2015, 4, 7))]
    """
    days = lit(days) if isinstance(days, int) else days
    raise NotImplementedError


@try_remote_functions
def date_sub(start: "AbstractColOrName", days: Union["AbstractColOrName", int]) -> Func:
    """
    Returns the date that is `days` days before `start`. If `days` is a negative value
    then these amount of days will be added to `start`.

    .. versionadded:: 1.5.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    start : :class:`~osos.Col` or str
        date AbstractCol to work on.
    days : :class:`~osos.Col` or str or int
        how many days before the given date to calculate.
        Accepts negative value as well to calculate forward in time.

    Returns
    -------
    :class:`~osos.Col`
        a date before/after given number of days.

    Examples
    --------
    >>> df = spark.createDataFrame([('2015-04-08', 2,)], ['dt', 'sub'])
    >>> df.select(date_sub(df.dt, 1).alias('prev_date')).collect()
    [Row(prev_date=datetime.date(2015, 4, 7))]
    >>> df.select(date_sub(df.dt, df.sub.cast('integer')).alias('prev_date')).collect()
    [Row(prev_date=datetime.date(2015, 4, 6))]
    >>> df.select(date_sub('dt', -1).alias('next_date')).collect()
    [Row(next_date=datetime.date(2015, 4, 9))]
    """
    days = lit(days) if isinstance(days, int) else days
    raise NotImplementedError


@try_remote_functions
def datediff(end: "AbstractColOrName", start: "AbstractColOrName") -> Func:
    """
    Returns the number of days from `start` to `end`.

    .. versionadded:: 1.5.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    end : :class:`~osos.Col` or str
        to date AbstractCol to work on.
    start : :class:`~osos.Col` or str
        from date AbstractCol to work on.

    Returns
    -------
    :class:`~osos.Col`
        difference in days between two dates.

    Examples
    --------
    >>> df = spark.createDataFrame([('2015-04-08','2015-05-10')], ['d1', 'd2'])
    >>> df.select(datediff(df.d2, df.d1).alias('diff')).collect()
    [Row(diff=32)]
    """
    raise NotImplementedError


@try_remote_functions
def add_months(
    start: "AbstractColOrName", months: Union["AbstractColOrName", int]
) -> Func:
    """
    Returns the date that is `months` months after `start`. If `months` is a negative value
    then these amount of months will be deducted from the `start`.

    .. versionadded:: 1.5.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    start : :class:`~osos.Col` or str
        date AbstractCol to work on.
    months : :class:`~osos.Col` or str or int
        how many months after the given date to calculate.
        Accepts negative value as well to calculate backwards.

    Returns
    -------
    :class:`~osos.Col`
        a date after/before given number of months.

    Examples
    --------
    >>> df = spark.createDataFrame([('2015-04-08', 2)], ['dt', 'add'])
    >>> df.select(add_months(df.dt, 1).alias('next_month')).collect()
    [Row(next_month=datetime.date(2015, 5, 8))]
    >>> df.select(add_months(df.dt, df.add.cast('integer')).alias('next_month')).collect()
    [Row(next_month=datetime.date(2015, 6, 8))]
    >>> df.select(add_months('dt', -2).alias('prev_month')).collect()
    [Row(prev_month=datetime.date(2015, 2, 8))]
    """
    months = lit(months) if isinstance(months, int) else months
    raise NotImplementedError


@try_remote_functions
def months_between(
    date1: "AbstractColOrName", date2: "AbstractColOrName", roundOff: bool = True
) -> Func:
    """
    Returns number of months between dates date1 and date2.
    If date1 is later than date2, then the result is positive.
    A whole number is returned if both inputs have the same day of month or both are the last day
    of their respective months. Otherwise, the difference is calculated assuming 31 days per month.
    The result is rounded off to 8 digits unless `roundOff` is set to `False`.

    .. versionadded:: 1.5.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    date1 : :class:`~osos.Col` or str
        first date AbstractCol.
    date2 : :class:`~osos.Col` or str
        second date AbstractCol.
    roundOff : bool, optional
        whether to round (to 8 digits) the final value or not (default: True).

    Returns
    -------
    :class:`~osos.Col`
        number of months between two dates.

    Examples
    --------
    >>> df = spark.createDataFrame([('1997-02-28 10:30:00', '1996-10-30')], ['date1', 'date2'])
    >>> df.select(months_between(df.date1, df.date2).alias('months')).collect()
    [Row(months=3.94959677)]
    >>> df.select(months_between(df.date1, df.date2, False).alias('months')).collect()
    [Row(months=3.9495967741935485)]
    """
    raise NotImplemented(
        "months_between", AbstractCol(date1), AbstractCol(date2), roundOff
    )


@try_remote_functions
def to_date(col: "AbstractColOrName", format: Optional[str] = None) -> Func:
    """Converts a :class:`~osos.Col` into :class:`pyspark.sql.types.DateType`
    using the optionally specified format. Specify formats according to `datetime pattern`_.
    By default, it follows casting rules to :class:`pyspark.sql.types.DateType` if the format
    is omitted. Equivalent to ``col.cast("date")``.

    .. _datetime pattern: https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html

    .. versionadded:: 2.2.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        input AbstractCol of values to convert.
    format: str, optional
        format to use to convert date values.

    Returns
    -------
    :class:`~osos.Col`
        date value as :class:`pyspark.sql.types.DateType` type.

    Examples
    --------
    >>> df = spark.createDataFrame([('1997-02-28 10:30:00',)], ['t'])
    >>> df.select(to_date(df.t).alias('date')).collect()
    [Row(date=datetime.date(1997, 2, 28))]

    >>> df = spark.createDataFrame([('1997-02-28 10:30:00',)], ['t'])
    >>> df.select(to_date(df.t, 'yyyy-MM-dd HH:mm:ss').alias('date')).collect()
    [Row(date=datetime.date(1997, 2, 28))]
    """
    if format is None:
        raise NotImplementedError
    else:
        raise NotImplementedError


@overload
def to_timestamp(col: "AbstractColOrName") -> Func:
    ...


@overload
def to_timestamp(col: "AbstractColOrName", format: str) -> Func:
    ...


@try_remote_functions
def to_timestamp(col: "AbstractColOrName", format: Optional[str] = None) -> Func:
    """Converts a :class:`~osos.Col` into :class:`pyspark.sql.types.TimestampType`
    using the optionally specified format. Specify formats according to `datetime pattern`_.
    By default, it follows casting rules to :class:`pyspark.sql.types.TimestampType` if the format
    is omitted. Equivalent to ``col.cast("timestamp")``.

    .. _datetime pattern: https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html

    .. versionadded:: 2.2.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        AbstractCol values to convert.
    format: str, optional
        format to use to convert timestamp values.

    Returns
    -------
    :class:`~osos.Col`
        timestamp value as :class:`pyspark.sql.types.TimestampType` type.

    Examples
    --------
    >>> df = spark.createDataFrame([('1997-02-28 10:30:00',)], ['t'])
    >>> df.select(to_timestamp(df.t).alias('dt')).collect()
    [Row(dt=datetime.datetime(1997, 2, 28, 10, 30))]

    >>> df = spark.createDataFrame([('1997-02-28 10:30:00',)], ['t'])
    >>> df.select(to_timestamp(df.t, 'yyyy-MM-dd HH:mm:ss').alias('dt')).collect()
    [Row(dt=datetime.datetime(1997, 2, 28, 10, 30))]
    """
    if format is None:
        raise NotImplementedError
    else:
        raise NotImplementedError


@try_remote_functions
def trunc(date: "AbstractColOrName", format: str) -> Func:
    """
    Returns date truncated to the unit specified by the format.

    .. versionadded:: 1.5.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    date : :class:`~osos.Col` or str
        input AbstractCol of values to truncate.
    format : str
        'year', 'yyyy', 'yy' to truncate by year,
        or 'month', 'mon', 'mm' to truncate by month
        Other options are: 'week', 'quarter'

    Returns
    -------
    :class:`~osos.Col`
        truncated date.

    Examples
    --------
    >>> df = spark.createDataFrame([('1997-02-28',)], ['d'])
    >>> df.select(trunc(df.d, 'year').alias('year')).collect()
    [Row(year=datetime.date(1997, 1, 1))]
    >>> df.select(trunc(df.d, 'mon').alias('month')).collect()
    [Row(month=datetime.date(1997, 2, 1))]
    """
    raise NotImplementedError


@try_remote_functions
def date_trunc(format: str, timestamp: "AbstractColOrName") -> Func:
    """
    Returns timestamp truncated to the unit specified by the format.

    .. versionadded:: 2.3.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    format : str
        'year', 'yyyy', 'yy' to truncate by year,
        'month', 'mon', 'mm' to truncate by month,
        'day', 'dd' to truncate by day,
        Other options are:
        'microsecond', 'millisecond', 'second', 'minute', 'hour', 'week', 'quarter'
    timestamp : :class:`~osos.Col` or str
        input AbstractCol of values to truncate.

    Returns
    -------
    :class:`~osos.Col`
        truncated timestamp.

    Examples
    --------
    >>> df = spark.createDataFrame([('1997-02-28 05:02:11',)], ['t'])
    >>> df.select(date_trunc('year', df.t).alias('year')).collect()
    [Row(year=datetime.datetime(1997, 1, 1, 0, 0))]
    >>> df.select(date_trunc('mon', df.t).alias('month')).collect()
    [Row(month=datetime.datetime(1997, 2, 1, 0, 0))]
    """
    raise NotImplementedError


@try_remote_functions
def next_day(date: "AbstractColOrName", dayOfWeek: str) -> Func:
    """
    Returns the first date which is later than the value of the date AbstractCol
    based on second `week day` argument.

    .. versionadded:: 1.5.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    date : :class:`~osos.Col` or str
        target AbstractCol to compute on.
    dayOfWeek : str
        day of the week, case-insensitive, accepts:
            "Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"

    Returns
    -------
    :class:`~osos.Col`
        the AbstractCol of computed results.

    Examples
    --------
    >>> df = spark.createDataFrame([('2015-07-27',)], ['d'])
    >>> df.select(next_day(df.d, 'Sun').alias('date')).collect()
    [Row(date=datetime.date(2015, 8, 2))]
    """
    raise NotImplementedError


@try_remote_functions
def last_day(date: "AbstractColOrName") -> Func:
    """
    Returns the last day of the month which the given date belongs to.

    .. versionadded:: 1.5.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    date : :class:`~osos.Col` or str
        target AbstractCol to compute on.

    Returns
    -------
    :class:`~osos.Col`
        last day of the month.

    Examples
    --------
    >>> df = spark.createDataFrame([('1997-02-10',)], ['d'])
    >>> df.select(last_day(df.d).alias('date')).collect()
    [Row(date=datetime.date(1997, 2, 28))]
    """
    raise NotImplementedError


@try_remote_functions
def from_unixtime(
    timestamp: "AbstractColOrName", format: str = "yyyy-MM-dd HH:mm:ss"
) -> Func:
    """
    Converts the number of seconds from unix epoch (1970-01-01 00:00:00 UTC) to a string
    representing the timestamp of that moment in the current system time zone in the given
    format.

    .. versionadded:: 1.5.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    timestamp : :class:`~osos.Col` or str
        AbstractCol of unix time values.
    format : str, optional
        format to use to convert to (default: yyyy-MM-dd HH:mm:ss)

    Returns
    -------
    :class:`~osos.Col`
        formatted timestamp as string.

    Examples
    --------
    >>> spark.conf.set("spark.sql.session.timeZone", "America/Los_Angeles")
    >>> time_df = spark.createDataFrame([(1428476400,)], ['unix_time'])
    >>> time_df.select(from_unixtime('unix_time').alias('ts')).collect()
    [Row(ts='2015-04-08 00:00:00')]
    >>> spark.conf.unset("spark.sql.session.timeZone")
    """
    raise NotImplementedError


@overload
def unix_timestamp(timestamp: "AbstractColOrName", format: str = ...) -> Func:
    ...


@overload
def unix_timestamp() -> Func:
    ...


@try_remote_functions
def unix_timestamp(
    timestamp: Optional["AbstractColOrName"] = None, format: str = "yyyy-MM-dd HH:mm:ss"
) -> Func:
    """
    Convert time string with given pattern ('yyyy-MM-dd HH:mm:ss', by default)
    to Unix time stamp (in seconds), using the default timezone and the default
    locale, returns null if failed.

    if `timestamp` is None, then it returns current timestamp.

    .. versionadded:: 1.5.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    timestamp : :class:`~osos.Col` or str, optional
        timestamps of string values.
    format : str, optional
        alternative format to use for converting (default: yyyy-MM-dd HH:mm:ss).

    Returns
    -------
    :class:`~osos.Col`
        unix time as long integer.

    Examples
    --------
    >>> spark.conf.set("spark.sql.session.timeZone", "America/Los_Angeles")
    >>> time_df = spark.createDataFrame([('2015-04-08',)], ['dt'])
    >>> time_df.select(unix_timestamp('dt', 'yyyy-MM-dd').alias('unix_time')).collect()
    [Row(unix_time=1428476400)]
    >>> spark.conf.unset("spark.sql.session.timeZone")
    """
    if timestamp is None:
        raise NotImplementedError
    raise NotImplementedError


@try_remote_functions
def from_utc_timestamp(timestamp: "AbstractColOrName", tz: "AbstractColOrName") -> Func:
    """
    This is a common function for databases supporting TIMESTAMP WITHOUT TIMEZONE. This function
    takes a timestamp which is timezone-agnostic, and interprets it as a timestamp in UTC, and
    renders that timestamp as a timestamp in the given time zone.

    However, timestamp in Spark represents number of microseconds from the Unix epoch, which is not
    timezone-agnostic. So in Spark this function just shift the timestamp value from UTC timezone to
    the given timezone.

    This function may return confusing result if the input is a string with timezone, e.g.
    '2018-03-13T06:18:23+00:00'. The reason is that, Spark firstly cast the string to timestamp
    according to the timezone in the string, and finally display the result by converting the
    timestamp to string according to the session local timezone.

    .. versionadded:: 1.5.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    timestamp : :class:`~osos.Col` or str
        the AbstractCol that contains timestamps
    tz : :class:`~osos.Col` or str
        A string detailing the time zone ID that the input should be adjusted to. It should
        be in the format of either region-based zone IDs or zone offsets. Region IDs must
        have the form 'area/city', such as 'America/Los_Angeles'. Zone offsets must be in
        the format '(+|-)HH:mm', for example '-08:00' or '+01:00'. Also 'UTC' and 'Z' are
        supported as aliases of '+00:00'. Other short names are not recommended to use
        because they can be ambiguous.

        .. versionchanged:: 2.4
           `tz` can take a :class:`~osos.Col` containing timezone ID strings.

    Returns
    -------
    :class:`~osos.Col`
        timestamp value represented in given timezone.

    Examples
    --------
    >>> df = spark.createDataFrame([('1997-02-28 10:30:00', 'JST')], ['ts', 'tz'])
    >>> df.select(from_utc_timestamp(df.ts, "PST").alias('local_time')).collect()
    [Row(local_time=datetime.datetime(1997, 2, 28, 2, 30))]
    >>> df.select(from_utc_timestamp(df.ts, df.tz).alias('local_time')).collect()
    [Row(local_time=datetime.datetime(1997, 2, 28, 19, 30))]
    """
    if isinstance(tz, AbstractCol):
        tz = AbstractCol(tz)
    raise NotImplementedError


@try_remote_functions
def to_utc_timestamp(timestamp: "AbstractColOrName", tz: "AbstractColOrName") -> Func:
    """
    This is a common function for databases supporting TIMESTAMP WITHOUT TIMEZONE. This function
    takes a timestamp which is timezone-agnostic, and interprets it as a timestamp in the given
    timezone, and renders that timestamp as a timestamp in UTC.

    However, timestamp in Spark represents number of microseconds from the Unix epoch, which is not
    timezone-agnostic. So in Spark this function just shift the timestamp value from the given
    timezone to UTC timezone.

    This function may return confusing result if the input is a string with timezone, e.g.
    '2018-03-13T06:18:23+00:00'. The reason is that, Spark firstly cast the string to timestamp
    according to the timezone in the string, and finally display the result by converting the
    timestamp to string according to the session local timezone.

    .. versionadded:: 1.5.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    timestamp : :class:`~osos.Col` or str
        the AbstractCol that contains timestamps
    tz : :class:`~osos.Col` or str
        A string detailing the time zone ID that the input should be adjusted to. It should
        be in the format of either region-based zone IDs or zone offsets. Region IDs must
        have the form 'area/city', such as 'America/Los_Angeles'. Zone offsets must be in
        the format '(+|-)HH:mm', for example '-08:00' or '+01:00'. Also 'UTC' and 'Z' are
        supported as aliases of '+00:00'. Other short names are not recommended to use
        because they can be ambiguous.

        .. versionchanged:: 2.4.0
           `tz` can take a :class:`~osos.Col` containing timezone ID strings.

    Returns
    -------
    :class:`~osos.Col`
        timestamp value represented in UTC timezone.

    Examples
    --------
    >>> df = spark.createDataFrame([('1997-02-28 10:30:00', 'JST')], ['ts', 'tz'])
    >>> df.select(to_utc_timestamp(df.ts, "PST").alias('utc_time')).collect()
    [Row(utc_time=datetime.datetime(1997, 2, 28, 18, 30))]
    >>> df.select(to_utc_timestamp(df.ts, df.tz).alias('utc_time')).collect()
    [Row(utc_time=datetime.datetime(1997, 2, 28, 1, 30))]
    """
    if isinstance(tz, AbstractCol):
        tz = AbstractCol(tz)
    raise NotImplementedError


@try_remote_functions
def timestamp_seconds(col: "AbstractColOrName") -> Func:
    """
    Converts the number of seconds from the Unix epoch (1970-01-01T00:00:00Z)
    to a timestamp.

    .. versionadded:: 3.1.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        unix time values.

    Returns
    -------
    :class:`~osos.Col`
        converted timestamp value.

    Examples
    --------
    >>> from pyspark.sql.functions import timestamp_seconds
    >>> spark.conf.set("spark.sql.session.timeZone", "UTC")
    >>> time_df = spark.createDataFrame([(1230219000,)], ['unix_time'])
    >>> time_df.select(timestamp_seconds(time_df.unix_time).alias('ts')).show()
    +-------------------+
    |                 ts|
    +-------------------+
    |2008-12-25 15:30:00|
    +-------------------+
    >>> time_df.select(timestamp_seconds('unix_time').alias('ts')).printSchema()
    root
     |-- ts: timestamp (nullable = true)
    >>> spark.conf.unset("spark.sql.session.timeZone")
    """

    raise NotImplementedError


@try_remote_functions
def window(
    timeAbstractCol: "AbstractColOrName",
    windowDuration: str,
    slideDuration: Optional[str] = None,
    startTime: Optional[str] = None,
) -> Func:
    """Bucketize rows into one or more time windows given a timestamp specifying AbstractCol. Window
    starts are inclusive but the window ends are exclusive, e.g. 12:05 will be in the window
    [12:05,12:10) but not in [12:00,12:05). Windows can support microsecond precision. Windows in
    the order of months are not supported.

    The time AbstractCol must be of :class:`pyspark.sql.types.TimestampType`.

    Durations are provided as strings, e.g. '1 second', '1 day 12 hours', '2 minutes'. Valid
    interval strings are 'week', 'day', 'hour', 'minute', 'second', 'millisecond', 'microsecond'.
    If the ``slideDuration`` is not provided, the windows will be tumbling windows.

    The startTime is the offset with respect to 1970-01-01 00:00:00 UTC with which to start
    window intervals. For example, in order to have hourly tumbling windows that start 15 minutes
    past the hour, e.g. 12:15-13:15, 13:15-14:15... provide `startTime` as `15 minutes`.

    The output AbstractCol will be a struct called 'window' by default with the nested AbstractCols 'start'
    and 'end', where 'start' and 'end' will be of :class:`pyspark.sql.types.TimestampType`.

    .. versionadded:: 2.0.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    timeAbstractCol : :class:`~osos.Col`
        The AbstractCol or the expression to use as the timestamp for windowing by time.
        The time AbstractCol must be of TimestampType or TimestampNTZType.
    windowDuration : str
        A string specifying the width of the window, e.g. `10 minutes`,
        `1 second`. Check `org.apache.spark.unsafe.types.CalendarInterval` for
        valid duration identifiers. Note that the duration is a fixed length of
        time, and does not vary over time according to a calendar. For example,
        `1 day` always means 86,400,000 milliseconds, not a calendar day.
    slideDuration : str, optional
        A new window will be generated every `slideDuration`. Must be less than
        or equal to the `windowDuration`. Check
        `org.apache.spark.unsafe.types.CalendarInterval` for valid duration
        identifiers. This duration is likewise absolute, and does not vary
        according to a calendar.
    startTime : str, optional
        The offset with respect to 1970-01-01 00:00:00 UTC with which to start
        window intervals. For example, in order to have hourly tumbling windows that
        start 15 minutes past the hour, e.g. 12:15-13:15, 13:15-14:15... provide
        `startTime` as `15 minutes`.

    Returns
    -------
    :class:`~osos.Col`
        the AbstractCol for computed results.

    Examples
    --------
    >>> import datetime
    >>> df = spark.createDataFrame(
    ...     [(datetime.datetime(2016, 3, 11, 9, 0, 7), 1)],
    ... ).toDF("date", "val")
    >>> w = df.groupBy(window("date", "5 seconds")).agg(sum("val").alias("sum"))
    >>> w.select(w.window.start.cast("string").alias("start"),
    ...          w.window.end.cast("string").alias("end"), "sum").collect()
    [Row(start='2016-03-11 09:00:05', end='2016-03-11 09:00:10', sum=1)]
    """

    def check_string_field(field, fieldName):  # type: ignore[no-untyped-def]
        if not field or type(field) is not str:
            raise ososTypeError(
                error_class="NOT_STR",
                message_parameters={
                    "arg_name": fieldName,
                    "arg_type": type(field).__name__,
                },
            )

    time_col = AbstractCol(timeAbstractCol)
    check_string_field(windowDuration, "windowDuration")
    if slideDuration and startTime:
        check_string_field(slideDuration, "slideDuration")
        check_string_field(startTime, "startTime")
        raise NotImplementedError
    elif slideDuration:
        check_string_field(slideDuration, "slideDuration")
        raise NotImplementedError
    elif startTime:
        check_string_field(startTime, "startTime")
        raise NotImplementedError
    else:
        raise NotImplementedError


@try_remote_functions
def window_time(
    windowAbstractCol: "AbstractColOrName",
) -> Func:
    """Computes the event time from a window AbstractCol. The AbstractCol window values are produced
    by window aggregating operators and are of type `STRUCT<start: TIMESTAMP, end: TIMESTAMP>`
    where start is inclusive and end is exclusive. The event time of records produced by window
    aggregating operators can be computed as ``window_time(window)`` and are
    ``window.end - lit(1).alias("microsecond")`` (as microsecond is the minimal supported event
    time precision). The window AbstractCol must be one produced by a window aggregating operator.

    .. versionadded:: 3.4.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    windowAbstractCol : :class:`~osos.Col`
        The window AbstractCol of a window aggregate records.

    Returns
    -------
    :class:`~osos.Col`
        the AbstractCol for computed results.

    Examples
    --------
    >>> import datetime
    >>> df = spark.createDataFrame(
    ...     [(datetime.datetime(2016, 3, 11, 9, 0, 7), 1)],
    ... ).toDF("date", "val")

    Group the data into 5 second time windows and aggregate as sum.

    >>> w = df.groupBy(window("date", "5 seconds")).agg(sum("val").alias("sum"))

    Extract the window event time using the window_time function.

    >>> w.select(
    ...     w.window.end.cast("string").alias("end"),
    ...     window_time(w.window).cast("string").alias("window_time"),
    ...     "sum"
    ... ).collect()
    [Row(end='2016-03-11 09:00:10', window_time='2016-03-11 09:00:09.999999', sum=1)]
    """
    window_col = AbstractCol(windowAbstractCol)
    raise NotImplementedError


@try_remote_functions
def session_window(
    timeAbstractCol: "AbstractColOrName", gapDuration: Union[AbstractCol, str]
) -> Func:
    """
    Generates session window given a timestamp specifying AbstractCol.
    Session window is one of dynamic windows, which means the length of window is varying
    according to the given inputs. The length of session window is defined as "the timestamp
    of latest input of the session + gap duration", so when the new inputs are bound to the
    current session window, the end time of session window can be expanded according to the new
    inputs.
    Windows can support microsecond precision. Windows in the order of months are not supported.
    For a streaming query, you may use the function `current_timestamp` to generate windows on
    processing time.
    gapDuration is provided as strings, e.g. '1 second', '1 day 12 hours', '2 minutes'. Valid
    interval strings are 'week', 'day', 'hour', 'minute', 'second', 'millisecond', 'microsecond'.
    It could also be a AbstractCol which can be evaluated to gap duration dynamically based on the
    input row.
    The output AbstractCol will be a struct called 'session_window' by default with the nested AbstractCols
    'start' and 'end', where 'start' and 'end' will be of :class:`pyspark.sql.types.TimestampType`.

    .. versionadded:: 3.2.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    timeAbstractCol : :class:`~osos.Col` or str
        The AbstractCol name or AbstractCol to use as the timestamp for windowing by time.
        The time AbstractCol must be of TimestampType or TimestampNTZType.
    gapDuration : :class:`~osos.Col` or str
        A Python string literal or AbstractCol specifying the timeout of the session. It could be
        static value, e.g. `10 minutes`, `1 second`, or an expression/UDF that specifies gap
        duration dynamically based on the input row.

    Returns
    -------
    :class:`~osos.Col`
        the AbstractCol for computed results.

    Examples
    --------
    >>> df = spark.createDataFrame([("2016-03-11 09:00:07", 1)]).toDF("date", "val")
    >>> w = df.groupBy(session_window("date", "5 seconds")).agg(sum("val").alias("sum"))
    >>> w.select(w.session_window.start.cast("string").alias("start"),
    ...          w.session_window.end.cast("string").alias("end"), "sum").collect()
    [Row(start='2016-03-11 09:00:07', end='2016-03-11 09:00:12', sum=1)]
    >>> w = df.groupBy(session_window("date", lit("5 seconds"))).agg(sum("val").alias("sum"))
    >>> w.select(w.session_window.start.cast("string").alias("start"),
    ...          w.session_window.end.cast("string").alias("end"), "sum").collect()
    [Row(start='2016-03-11 09:00:07', end='2016-03-11 09:00:12', sum=1)]
    """

    def check_field(field: Union[AbstractCol, str], fieldName: str) -> None:
        if field is None or not isinstance(field, (str, AbstractCol)):
            raise ososTypeError(
                error_class="NOT_AbstractCol_OR_STR",
                message_parameters={
                    "arg_name": fieldName,
                    "arg_type": type(field).__name__,
                },
            )

    time_col = AbstractCol(timeAbstractCol)
    check_field(gapDuration, "gapDuration")
    gap_duration = (
        gapDuration if isinstance(gapDuration, str) else AbstractCol(gapDuration)
    )
    raise NotImplementedError


# ---------------------------- misc functions ----------------------------------


@try_remote_functions
def crc32(col: "AbstractColOrName") -> Func:
    """
    Calculates the cyclic redundancy check value  (CRC32) of a binary AbstractCol and
    returns the value as a bigint.

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        target AbstractCol to compute on.

    Returns
    -------
    :class:`~osos.Col`
        the AbstractCol for computed results.

    .. versionadded:: 1.5.0

    Examples
    --------
    >>> spark.createDataFrame([('ABC',)], ['a']).select(crc32('a').alias('crc32')).collect()
    [Row(crc32=2743272264)]
    """
    raise NotImplementedError


@try_remote_functions
def md5(col: "AbstractColOrName") -> Func:
    """Calculates the MD5 digest and returns the value as a 32 character hex string.

    .. versionadded:: 1.5.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        target AbstractCol to compute on.

    Returns
    -------
    :class:`~osos.Col`
        the AbstractCol for computed results.

    Examples
    --------
    >>> spark.createDataFrame([('ABC',)], ['a']).select(md5('a').alias('hash')).collect()
    [Row(hash='902fbdd2b1df0c4f70b4a5d23525e932')]
    """
    raise NotImplementedError


@try_remote_functions
def sha1(col: "AbstractColOrName") -> Func:
    """Returns the hex string result of SHA-1.

    .. versionadded:: 1.5.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        target AbstractCol to compute on.

    Returns
    -------
    :class:`~osos.Col`
        the AbstractCol for computed results.

    Examples
    --------
    >>> spark.createDataFrame([('ABC',)], ['a']).select(sha1('a').alias('hash')).collect()
    [Row(hash='3c01bdbb26f358bab27f267924aa2c9a03fcfdb8')]
    """
    raise NotImplementedError


@try_remote_functions
def sha2(col: "AbstractColOrName", numBits: int) -> Func:
    """Returns the hex string result of SHA-2 family of hash functions (SHA-224, SHA-256, SHA-384,
    and SHA-512). The numBits indicates the desired bit length of the result, which must have a
    value of 224, 256, 384, 512, or 0 (which is equivalent to 256).

    .. versionadded:: 1.5.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        target AbstractCol to compute on.
    numBits : int
        the desired bit length of the result, which must have a
        value of 224, 256, 384, 512, or 0 (which is equivalent to 256).

    Returns
    -------
    :class:`~osos.Col`
        the AbstractCol for computed results.

    Examples
    --------
    >>> df = spark.createDataFrame([["Alice"], ["Bob"]], ["name"])
    >>> df.withAbstractCol("sha2", sha2(df.name, 256)).show(truncate=False)
    +-----+----------------------------------------------------------------+
    |name |sha2                                                            |
    +-----+----------------------------------------------------------------+
    |Alice|3bc51062973c458d5a6f2d8d64a023246354ad7e064b1e4e009ec8a0699a3043|
    |Bob  |cd9fb1e148ccd8442e5aa74904cc73bf6fb54d1d54d333bd596aa9bb4bb4e961|
    +-----+----------------------------------------------------------------+
    """
    raise NotImplementedError


@try_remote_functions
def hash(*cols: "AbstractColOrName") -> Func:
    """Calculates the hash code of given AbstractCols, and returns the result as an int AbstractCol.

    .. versionadded:: 2.0.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    cols : :class:`~osos.Col` or str
        one or more AbstractCols to compute on.

    Returns
    -------
    :class:`~osos.Col`
        hash value as int AbstractCol.

    Examples
    --------
    >>> df = spark.createDataFrame([('ABC', 'DEF')], ['c1', 'c2'])

    Hash for one AbstractCol

    >>> df.select(hash('c1').alias('hash')).show()
    +----------+
    |      hash|
    +----------+
    |-757602832|
    +----------+

    Two or more AbstractCols

    >>> df.select(hash('c1', 'c2').alias('hash')).show()
    +---------+
    |     hash|
    +---------+
    |599895104|
    +---------+
    """
    raise NotImplementedError


@try_remote_functions
def xxhash64(*cols: "AbstractColOrName") -> Func:
    """Calculates the hash code of given AbstractCols using the 64-bit variant of the xxHash algorithm,
    and returns the result as a long AbstractCol. The hash computation uses an initial seed of 42.

    .. versionadded:: 3.0.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    cols : :class:`~osos.Col` or str
        one or more AbstractCols to compute on.

    Returns
    -------
    :class:`~osos.Col`
        hash value as long AbstractCol.

    Examples
    --------
    >>> df = spark.createDataFrame([('ABC', 'DEF')], ['c1', 'c2'])

    Hash for one AbstractCol

    >>> df.select(xxhash64('c1').alias('hash')).show()
    +-------------------+
    |               hash|
    +-------------------+
    |4105715581806190027|
    +-------------------+

    Two or more AbstractCols

    >>> df.select(xxhash64('c1', 'c2').alias('hash')).show()
    +-------------------+
    |               hash|
    +-------------------+
    |3233247871021311208|
    +-------------------+
    """
    raise NotImplementedError


@try_remote_functions
def assert_true(
    col: "AbstractColOrName", errMsg: Optional[Union[AbstractCol, str]] = None
) -> Func:
    """
    Returns `null` if the input AbstractCol is `true`; throws an exception
    with the provided error message otherwise.

    .. versionadded:: 3.1.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        AbstractCol name or AbstractCol that represents the input AbstractCol to test
    errMsg : :class:`~osos.Col` or str, optional
        A Python string literal or AbstractCol containing the error message

    Returns
    -------
    :class:`~osos.Col`
        `null` if the input AbstractCol is `true` otherwise throws an error with specified message.

    Examples
    --------
    >>> df = spark.createDataFrame([(0,1)], ['a', 'b'])
    >>> df.select(assert_true(df.a < df.b).alias('r')).collect()
    [Row(r=None)]
    >>> df.select(assert_true(df.a < df.b, df.a).alias('r')).collect()
    [Row(r=None)]
    >>> df.select(assert_true(df.a < df.b, 'error').alias('r')).collect()
    [Row(r=None)]
    >>> df.select(assert_true(df.a > df.b, 'My error msg').alias('r')).collect() # doctest: +SKIP
    ...
    java.lang.RuntimeException: My error msg
    ...
    """
    if errMsg is None:
        raise NotImplementedError
    if not isinstance(errMsg, (str, AbstractCol)):
        raise ososTypeError(
            error_class="NOT_AbstractCol_OR_STR",
            message_parameters={
                "arg_name": "errMsg",
                "arg_type": type(errMsg).__name__,
            },
        )

    errMsg = (
        _create_AbstractCol_from_literal(errMsg)
        if isinstance(errMsg, str)
        else AbstractCol(errMsg)
    )
    raise NotImplementedError


@try_remote_functions
def raise_error(errMsg: Union[AbstractCol, str]) -> Func:
    """
    Throws an exception with the provided error message.

    .. versionadded:: 3.1.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    errMsg : :class:`~osos.Col` or str
        A Python string literal or AbstractCol containing the error message

    Returns
    -------
    :class:`~osos.Col`
        throws an error with specified message.

    Examples
    --------
    >>> df = spark.range(1)
    >>> df.select(raise_error("My error message")).show() # doctest: +SKIP
    ...
    java.lang.RuntimeException: My error message
    ...
    """
    if not isinstance(errMsg, (str, AbstractCol)):
        raise OsosTypeError(
            error_class="NOT_AbstractCol_OR_STR",
            message_parameters={
                "arg_name": "errMsg",
                "arg_type": type(errMsg).__name__,
            },
        )

    errMsg = AbstractLit(errMsg) if isinstance(errMsg, str) else AbstractCol(errMsg)
    raise NotImplementedError


# ---------------------- String/Binary functions ------------------------------


@try_remote_functions
def upper(col: "AbstractColOrName") -> Func:
    """
    Converts a string expression to upper case.

    .. versionadded:: 1.5.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        target AbstractCol to work on.

    Returns
    -------
    :class:`~osos.Col`
        upper case values.

    Examples
    --------
    >>> df = spark.createDataFrame(["Spark", "PySpark", "Pandas API"], "STRING")
    >>> df.select(upper("value")).show()
    +------------+
    |upper(value)|
    +------------+
    |       SPARK|
    |     PYSPARK|
    |  PANDAS API|
    +------------+
    """
    raise NotImplementedError


@try_remote_functions
def lower(col: "AbstractColOrName") -> Func:
    """
    Converts a string expression to lower case.

    .. versionadded:: 1.5.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        target AbstractCol to work on.

    Returns
    -------
    :class:`~osos.Col`
        lower case values.

    Examples
    --------
    >>> df = spark.createDataFrame(["Spark", "PySpark", "Pandas API"], "STRING")
    >>> df.select(lower("value")).show()
    +------------+
    |lower(value)|
    +------------+
    |       spark|
    |     pyspark|
    |  pandas api|
    +------------+
    """
    if isinstance(col, str):
        col = AbstractCol(col)

    return Func(lower_func, col)


@try_remote_functions
def ascii(col: "AbstractColOrName") -> Func:
    """
    Computes the numeric value of the first character of the string AbstractCol.

    .. versionadded:: 1.5.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        target AbstractCol to work on.

    Returns
    -------
    :class:`~osos.Col`
        numeric value.

    Examples
    --------
    >>> df = spark.createDataFrame(["Spark", "PySpark", "Pandas API"], "STRING")
    >>> df.select(ascii("value")).show()
    +------------+
    |ascii(value)|
    +------------+
    |          83|
    |          80|
    |          80|
    +------------+
    """
    raise NotImplementedError


@try_remote_functions
def base64(col: "AbstractColOrName") -> Func:
    """
    Computes the BASE64 encoding of a binary AbstractCol and returns it as a string AbstractCol.

    .. versionadded:: 1.5.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        target AbstractCol to work on.

    Returns
    -------
    :class:`~osos.Col`
        BASE64 encoding of string value.

    Examples
    --------
    >>> df = spark.createDataFrame(["Spark", "PySpark", "Pandas API"], "STRING")
    >>> df.select(base64("value")).show()
    +----------------+
    |   base64(value)|
    +----------------+
    |        U3Bhcms=|
    |    UHlTcGFyaw==|
    |UGFuZGFzIEFQSQ==|
    +----------------+
    """
    raise NotImplementedError


@try_remote_functions
def unbase64(col: "AbstractColOrName") -> Func:
    """
    Decodes a BASE64 encoded string AbstractCol and returns it as a binary AbstractCol.

    .. versionadded:: 1.5.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        target AbstractCol to work on.

    Returns
    -------
    :class:`~osos.Col`
        encoded string value.

    Examples
    --------
    >>> df = spark.createDataFrame(["U3Bhcms=",
    ...                             "UHlTcGFyaw==",
    ...                             "UGFuZGFzIEFQSQ=="], "STRING")
    >>> df.select(unbase64("value")).show()
    +--------------------+
    |     unbase64(value)|
    +--------------------+
    |    [53 70 61 72 6B]|
    |[50 79 53 70 61 7...|
    |[50 61 6E 64 61 7...|
    +--------------------+
    """
    raise NotImplementedError


@try_remote_functions
def ltrim(col: "AbstractColOrName") -> Func:
    """
    Trim the spaces from left end for the specified string value.

    .. versionadded:: 1.5.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        target AbstractCol to work on.

    Returns
    -------
    :class:`~osos.Col`
        left trimmed values.

    Examples
    --------
    >>> df = spark.createDataFrame(["   Spark", "Spark  ", " Spark"], "STRING")
    >>> df.select(ltrim("value").alias("r")).withAbstractCol("length", length("r")).show()
    +-------+------+
    |      r|length|
    +-------+------+
    |  Spark|     5|
    |Spark  |     7|
    |  Spark|     5|
    +-------+------+
    """
    raise NotImplementedError


@try_remote_functions
def rtrim(col: "AbstractColOrName") -> Func:
    """
    Trim the spaces from right end for the specified string value.

    .. versionadded:: 1.5.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        target AbstractCol to work on.

    Returns
    -------
    :class:`~osos.Col`
        right trimmed values.

    Examples
    --------
    >>> df = spark.createDataFrame(["   Spark", "Spark  ", " Spark"], "STRING")
    >>> df.select(rtrim("value").alias("r")).withAbstractCol("length", length("r")).show()
    +--------+------+
    |       r|length|
    +--------+------+
    |   Spark|     8|
    |   Spark|     5|
    |   Spark|     6|
    +--------+------+
    """
    raise NotImplementedError


@try_remote_functions
def trim(col: "AbstractColOrName") -> Func:
    """
    Trim the spaces from both ends for the specified string AbstractCol.

    .. versionadded:: 1.5.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        target AbstractCol to work on.

    Returns
    -------
    :class:`~osos.Col`
        trimmed values from both sides.

    Examples
    --------
    >>> df = spark.createDataFrame(["   Spark", "Spark  ", " Spark"], "STRING")
    >>> df.select(trim("value").alias("r")).withAbstractCol("length", length("r")).show()
    +-----+------+
    |    r|length|
    +-----+------+
    |Spark|     5|
    |Spark|     5|
    |Spark|     5|
    +-----+------+
    """
    raise NotImplementedError


@try_remote_functions
def concat_ws(sep: str, *cols: "AbstractColOrName") -> Func:
    """
    Concatenates multiple input string AbstractCols together into a single string AbstractCol,
    using the given separator.

    .. versionadded:: 1.5.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    sep : str
        words separator.
    cols : :class:`~osos.Col` or str
        list of AbstractCols to work on.

    Returns
    -------
    :class:`~osos.Col`
        string of concatenated words.

    Examples
    --------
    >>> df = spark.createDataFrame([('abcd','123')], ['s', 'd'])
    >>> df.select(concat_ws('-', df.s, df.d).alias('s')).collect()
    [Row(s='abcd-123')]
    """
    raise NotImplementedError


@try_remote_functions
def decode(col: "AbstractColOrName", charset: str) -> Func:
    """
    Computes the first argument into a string from a binary using the provided character set
    (one of 'US-ASCII', 'ISO-8859-1', 'UTF-8', 'UTF-16BE', 'UTF-16LE', 'UTF-16').

    .. versionadded:: 1.5.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        target AbstractCol to work on.
    charset : str
        charset to use to decode to.

    Returns
    -------
    :class:`~osos.Col`
        the AbstractCol for computed results.

    Examples
    --------
    >>> df = spark.createDataFrame([('abcd',)], ['a'])
    >>> df.select(decode("a", "UTF-8")).show()
    +----------------+
    |decode(a, UTF-8)|
    +----------------+
    |            abcd|
    +----------------+
    """
    raise NotImplementedError


@try_remote_functions
def encode(col: "AbstractColOrName", charset: str) -> Func:
    """
    Computes the first argument into a binary from a string using the provided character set
    (one of 'US-ASCII', 'ISO-8859-1', 'UTF-8', 'UTF-16BE', 'UTF-16LE', 'UTF-16').

    .. versionadded:: 1.5.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        target AbstractCol to work on.
    charset : str
        charset to use to encode.

    Returns
    -------
    :class:`~osos.Col`
        the AbstractCol for computed results.

    Examples
    --------
    >>> df = spark.createDataFrame([('abcd',)], ['c'])
    >>> df.select(encode("c", "UTF-8")).show()
    +----------------+
    |encode(c, UTF-8)|
    +----------------+
    |   [61 62 63 64]|
    +----------------+
    """
    raise NotImplementedError


@try_remote_functions
def format_number(col: "AbstractColOrName", d: int) -> Func:
    """
    Formats the number X to a format like '#,--#,--#.--', rounded to d decimal places
    with HALF_EVEN round mode, and returns the result as a string.

    .. versionadded:: 1.5.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        the AbstractCol name of the numeric value to be formatted
    d : int
        the N decimal places

    Returns
    -------
    :class:`~osos.Col`
        the AbstractCol of formatted results.

    >>> spark.createDataFrame([(5,)], ['a']).select(format_number('a', 4).alias('v')).collect()
    [Row(v='5.0000')]
    """
    raise NotImplementedError


@try_remote_functions
def format_string(format: str, *cols: "AbstractColOrName") -> Func:
    """
    Formats the arguments in printf-style and returns the result as a string AbstractCol.

    .. versionadded:: 1.5.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    format : str
        string that can contain embedded format tags and used as result AbstractCol's value
    cols : :class:`~osos.Col` or str
        AbstractCol names or :class:`~osos.Col`\\s to be used in formatting

    Returns
    -------
    :class:`~osos.Col`
        the AbstractCol of formatted results.

    Examples
    --------
    >>> df = spark.createDataFrame([(5, "hello")], ['a', 'b'])
    >>> df.select(format_string('%d %s', df.a, df.b).alias('v')).collect()
    [Row(v='5 hello')]
    """
    raise NotImplementedError


@try_remote_functions
def instr(str: "AbstractColOrName", substr: str) -> Func:
    """
    Locate the position of the first occurrence of substr AbstractCol in the given string.
    Returns null if either of the arguments are null.

    .. versionadded:: 1.5.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Notes
    -----
    The position is not zero based, but 1 based index. Returns 0 if substr
    could not be found in str.

    Parameters
    ----------
    str : :class:`~osos.Col` or str
        target AbstractCol to work on.
    substr : str
        substring to look for.

    Returns
    -------
    :class:`~osos.Col`
        location of the first occurence of the substring as integer.

    Examples
    --------
    >>> df = spark.createDataFrame([('abcd',)], ['s',])
    >>> df.select(instr(df.s, 'b').alias('s')).collect()
    [Row(s=2)]
    """
    raise NotImplementedError


@try_remote_functions
def overlay(
    src: "AbstractColOrName",
    replace: "AbstractColOrName",
    pos: Union["AbstractColOrName", int],
    len: Union["AbstractColOrName", int] = -1,
) -> Func:
    """
    Overlay the specified portion of `src` with `replace`,
    starting from byte position `pos` of `src` and proceeding for `len` bytes.

    .. versionadded:: 3.0.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    src : :class:`~osos.Col` or str
        AbstractCol name or AbstractCol containing the string that will be replaced
    replace : :class:`~osos.Col` or str
        AbstractCol name or AbstractCol containing the substitution string
    pos : :class:`~osos.Col` or str or int
        AbstractCol name, AbstractCol, or int containing the starting position in src
    len : :class:`~osos.Col` or str or int, optional
        AbstractCol name, AbstractCol, or int containing the number of bytes to replace in src
        string by 'replace' defaults to -1, which represents the length of the 'replace' string

    Returns
    -------
    :class:`~osos.Col`
        string with replaced values.

    Examples
    --------
    >>> df = spark.createDataFrame([("SPARK_SQL", "CORE")], ("x", "y"))
    >>> df.select(overlay("x", "y", 7).alias("overlayed")).collect()
    [Row(overlayed='SPARK_CORE')]
    >>> df.select(overlay("x", "y", 7, 0).alias("overlayed")).collect()
    [Row(overlayed='SPARK_CORESQL')]
    >>> df.select(overlay("x", "y", 7, 2).alias("overlayed")).collect()
    [Row(overlayed='SPARK_COREL')]
    """
    if not isinstance(pos, (int, str, AbstractCol)):
        raise ososTypeError(
            error_class="NOT_AbstractCol_OR_INT_OR_STR",
            message_parameters={"arg_name": "pos", "arg_type": type(pos).__name__},
        )
    if len is not None and not isinstance(len, (int, str, AbstractCol)):
        raise ososTypeError(
            error_class="NOT_AbstractCol_OR_INT_OR_STR",
            message_parameters={"arg_name": "len", "arg_type": type(len).__name__},
        )

    pos = lit(pos) if isinstance(pos, int) else AbstractCol(pos)
    len = lit(len) if isinstance(len, int) else AbstractCol(len)

    raise NotImplementedError


@try_remote_functions
def sentences(
    string: "AbstractColOrName",
    language: Optional["AbstractColOrName"] = None,
    country: Optional["AbstractColOrName"] = None,
) -> Func:
    """
    Splits a string into arrays of sentences, where each sentence is an array of words.
    The 'language' and 'country' arguments are optional, and if omitted, the default locale is used.

    .. versionadded:: 3.2.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    string : :class:`~osos.Col` or str
        a string to be split
    language : :class:`~osos.Col` or str, optional
        a language of the locale
    country : :class:`~osos.Col` or str, optional
        a country of the locale

    Returns
    -------
    :class:`~osos.Col`
        arrays of split sentences.

    Examples
    --------
    >>> df = spark.createDataFrame([["This is an example sentence."]], ["string"])
    >>> df.select(sentences(df.string, lit("en"), lit("US"))).show(truncate=False)
    +-----------------------------------+
    |sentences(string, en, US)          |
    +-----------------------------------+
    |[[This, is, an, example, sentence]]|
    +-----------------------------------+
    >>> df = spark.createDataFrame([["Hello world. How are you?"]], ["s"])
    >>> df.select(sentences("s")).show(truncate=False)
    +---------------------------------+
    |sentences(s, , )                 |
    +---------------------------------+
    |[[Hello, world], [How, are, you]]|
    +---------------------------------+
    """
    if language is None:
        language = lit("")
    if country is None:
        country = lit("")

    raise NotImplementedError


@try_remote_functions
def substring(str: "AbstractColOrName", pos: int, len: int) -> Func:
    """
    Substring starts at `pos` and is of length `len` when str is String type or
    returns the slice of byte array that starts at `pos` in byte and is of length `len`
    when str is Binary type.

    .. versionadded:: 1.5.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Notes
    -----
    The position is not zero based, but 1 based index.

    Parameters
    ----------
    str : :class:`~osos.Col` or str
        target AbstractCol to work on.
    pos : int
        starting position in str.
    len : int
        length of chars.

    Returns
    -------
    :class:`~osos.Col`
        substring of given value.

    Examples
    --------
    >>> df = spark.createDataFrame([('abcd',)], ['s',])
    >>> df.select(substring(df.s, 1, 2).alias('s')).collect()
    [Row(s='ab')]
    """
    raise NotImplementedError


@try_remote_functions
def substring_index(str: "AbstractColOrName", delim: str, count: int) -> Func:
    """
    Returns the substring from string str before count occurrences of the delimiter delim.
    If count is positive, everything the left of the final delimiter (counting from left) is
    returned. If count is negative, every to the right of the final delimiter (counting from the
    right) is returned. substring_index performs a case-sensitive match when searching for delim.

    .. versionadded:: 1.5.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    str : :class:`~osos.Col` or str
        target AbstractCol to work on.
    delim : str
        delimiter of values.
    count : int
        number of occurences.

    Returns
    -------
    :class:`~osos.Col`
        substring of given value.

    Examples
    --------
    >>> df = spark.createDataFrame([('a.b.c.d',)], ['s'])
    >>> df.select(substring_index(df.s, '.', 2).alias('s')).collect()
    [Row(s='a.b')]
    >>> df.select(substring_index(df.s, '.', -3).alias('s')).collect()
    [Row(s='b.c.d')]
    """
    raise NotImplementedError


@try_remote_functions
def levenshtein(left: "AbstractColOrName", right: "AbstractColOrName") -> Func:
    """Computes the Levenshtein distance of the two given strings.

    .. versionadded:: 1.5.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    left : :class:`~osos.Col` or str
        first AbstractCol value.
    right : :class:`~osos.Col` or str
        second AbstractCol value.

    Returns
    -------
    :class:`~osos.Col`
        Levenshtein distance as integer value.

    Examples
    --------
    >>> df0 = spark.createDataFrame([('kitten', 'sitting',)], ['l', 'r'])
    >>> df0.select(levenshtein('l', 'r').alias('d')).collect()
    [Row(d=3)]
    """
    raise NotImplementedError


@try_remote_functions
def locate(substr: str, str: "AbstractColOrName", pos: int = 1) -> Func:
    """
    Locate the position of the first occurrence of substr in a string AbstractCol, after position pos.

    .. versionadded:: 1.5.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    substr : str
        a string
    str : :class:`~osos.Col` or str
        a AbstractCol of :class:`pyspark.sql.types.StringType`
    pos : int, optional
        start position (zero based)

    Returns
    -------
    :class:`~osos.Col`
        position of the substring.

    Notes
    -----
    The position is not zero based, but 1 based index. Returns 0 if substr
    could not be found in str.

    Examples
    --------
    >>> df = spark.createDataFrame([('abcd',)], ['s',])
    >>> df.select(locate('b', df.s, 1).alias('s')).collect()
    [Row(s=2)]
    """
    raise NotImplementedError


@try_remote_functions
def lpad(col: "AbstractColOrName", len: int, pad: str) -> Func:
    """
    Left-pad the string AbstractCol to width `len` with `pad`.

    .. versionadded:: 1.5.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        target AbstractCol to work on.
    len : int
        length of the final string.
    pad : str
        chars to prepend.

    Returns
    -------
    :class:`~osos.Col`
        left padded result.

    Examples
    --------
    >>> df = spark.createDataFrame([('abcd',)], ['s',])
    >>> df.select(lpad(df.s, 6, '#').alias('s')).collect()
    [Row(s='##abcd')]
    """
    raise NotImplementedError


@try_remote_functions
def rpad(col: "AbstractColOrName", len: int, pad: str) -> Func:
    """
    Right-pad the string AbstractCol to width `len` with `pad`.

    .. versionadded:: 1.5.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        target AbstractCol to work on.
    len : int
        length of the final string.
    pad : str
        chars to append.

    Returns
    -------
    :class:`~osos.Col`
        right padded result.

    Examples
    --------
    >>> df = spark.createDataFrame([('abcd',)], ['s',])
    >>> df.select(rpad(df.s, 6, '#').alias('s')).collect()
    [Row(s='abcd##')]
    """
    raise NotImplementedError


@try_remote_functions
def repeat(col: "AbstractColOrName", n: int) -> Func:
    """
    Repeats a string AbstractCol n times, and returns it as a new string AbstractCol.

    .. versionadded:: 1.5.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        target AbstractCol to work on.
    n : int
        number of times to repeat value.

    Returns
    -------
    :class:`~osos.Col`
        string with repeated values.

    Examples
    --------
    >>> df = spark.createDataFrame([('ab',)], ['s',])
    >>> df.select(repeat(df.s, 3).alias('s')).collect()
    [Row(s='ababab')]
    """
    raise NotImplementedError


@try_remote_functions
def split(str: "AbstractColOrName", pattern: str, limit: int = -1) -> Func:
    """
    Splits str around matches of the given pattern.

    .. versionadded:: 1.5.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    str : :class:`~osos.Col` or str
        a string expression to split
    pattern : str
        a string representing a regular expression. The regex string should be
        a Java regular expression.
    limit : int, optional
        an integer which controls the number of times `pattern` is applied.

        * ``limit > 0``: The resulting array's length will not be more than `limit`, and the
                         resulting array's last entry will contain all input beyond the last
                         matched pattern.
        * ``limit <= 0``: `pattern` will be applied as many times as possible, and the resulting
                          array can be of any size.

        .. versionchanged:: 3.0
           `split` now takes an optional `limit` field. If not provided, default limit value is -1.

    Returns
    -------
    :class:`~osos.Col`
        array of separated strings.

    Examples
    --------
    >>> df = spark.createDataFrame([('oneAtwoBthreeC',)], ['s',])
    >>> df.select(split(df.s, '[ABC]', 2).alias('s')).collect()
    [Row(s=['one', 'twoBthreeC'])]
    >>> df.select(split(df.s, '[ABC]', -1).alias('s')).collect()
    [Row(s=['one', 'two', 'three', ''])]
    """
    raise NotImplementedError


@try_remote_functions
def regexp_extract(str: "AbstractColOrName", pattern: str, idx: int) -> Func:
    r"""Extract a specific group matched by a Java regex, from the specified string AbstractCol.
    If the regex did not match, or the specified group did not match, an empty string is returned.

    .. versionadded:: 1.5.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    str : :class:`~osos.Col` or str
        target AbstractCol to work on.
    pattern : str
        regex pattern to apply.
    idx : int
        matched group id.

    Returns
    -------
    :class:`~osos.Col`
        matched value specified by `idx` group id.

    Examples
    --------
    >>> df = spark.createDataFrame([('100-200',)], ['str'])
    >>> df.select(regexp_extract('str', r'(\d+)-(\d+)', 1).alias('d')).collect()
    [Row(d='100')]
    >>> df = spark.createDataFrame([('foo',)], ['str'])
    >>> df.select(regexp_extract('str', r'(\d+)', 1).alias('d')).collect()
    [Row(d='')]
    >>> df = spark.createDataFrame([('aaaac',)], ['str'])
    >>> df.select(regexp_extract('str', '(a+)(b)?(c)', 2).alias('d')).collect()
    [Row(d='')]
    """
    raise NotImplementedError


@try_remote_functions
def regexp_replace(
    string: "AbstractColOrName",
    pattern: Union[str, AbstractCol],
    replacement: Union[str, AbstractCol],
) -> Func:
    r"""Replace all substrings of the specified string value that match regexp with replacement.

    .. versionadded:: 1.5.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    string : :class:`~osos.Col` or str
        AbstractCol name or AbstractCol containing the string value
    pattern : :class:`~osos.Col` or str
        AbstractCol object or str containing the regexp pattern
    replacement : :class:`~osos.Col` or str
        AbstractCol object or str containing the replacement

    Returns
    -------
    :class:`~osos.Col`
        string with all substrings replaced.

    Examples
    --------
    >>> df = spark.createDataFrame([("100-200", r"(\d+)", "--")], ["str", "pattern", "replacement"])
    >>> df.select(regexp_replace('str', r'(\d+)', '--').alias('d')).collect()
    [Row(d='-----')]
    >>> df.select(regexp_replace("str", col("pattern"), col("replacement")).alias('d')).collect()
    [Row(d='-----')]
    """
    if isinstance(pattern, str):
        pattern_col = lit(pattern)
    else:
        pattern_col = AbstractCol(pattern)
    if isinstance(replacement, str):
        replacement_col = lit(replacement)
    else:
        replacement_col = AbstractCol(replacement)
    raise NotImplementedError


@try_remote_functions
def initcap(col: "AbstractColOrName") -> Func:
    """Translate the first letter of each word to upper case in the sentence.

    .. versionadded:: 1.5.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        target AbstractCol to work on.

    Returns
    -------
    :class:`~osos.Col`
        string with all first letters are uppercase in each word.

    Examples
    --------
    >>> spark.createDataFrame([('ab cd',)], ['a']).select(initcap("a").alias('v')).collect()
    [Row(v='Ab Cd')]
    """
    raise NotImplementedError


@try_remote_functions
def soundex(col: "AbstractColOrName") -> Func:
    """
    Returns the SoundEx encoding for a string

    .. versionadded:: 1.5.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        target AbstractCol to work on.

    Returns
    -------
    :class:`~osos.Col`
        SoundEx encoded string.

    Examples
    --------
    >>> df = spark.createDataFrame([("Peters",),("Uhrbach",)], ['name'])
    >>> df.select(soundex(df.name).alias("soundex")).collect()
    [Row(soundex='P362'), Row(soundex='U612')]
    """
    raise NotImplementedError


@try_remote_functions
def bin(col: "AbstractColOrName") -> Func:
    """Returns the string representation of the binary value of the given AbstractCol.

    .. versionadded:: 1.5.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        target AbstractCol to work on.

    Returns
    -------
    :class:`~osos.Col`
        binary representation of given value as string.

    Examples
    --------
    >>> df = spark.createDataFrame([2,5], "INT")
    >>> df.select(bin(df.value).alias('c')).collect()
    [Row(c='10'), Row(c='101')]
    """
    raise NotImplementedError


@try_remote_functions
def hex(col: "AbstractColOrName") -> Func:
    """Computes hex value of the given AbstractCol, which could be :class:`pyspark.sql.types.StringType`,
    :class:`pyspark.sql.types.BinaryType`, :class:`pyspark.sql.types.IntegerType` or
    :class:`pyspark.sql.types.LongType`.

    .. versionadded:: 1.5.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        target AbstractCol to work on.

    Returns
    -------
    :class:`~osos.Col`
        hexadecimal representation of given value as string.

    Examples
    --------
    >>> spark.createDataFrame([('ABC', 3)], ['a', 'b']).select(hex('a'), hex('b')).collect()
    [Row(hex(a)='414243', hex(b)='3')]
    """
    raise NotImplementedError


@try_remote_functions
def unhex(col: "AbstractColOrName") -> Func:
    """Inverse of hex. Interprets each pair of characters as a hexadecimal number
    and converts to the byte representation of number.

    .. versionadded:: 1.5.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        target AbstractCol to work on.

    Returns
    -------
    :class:`~osos.Col`
        string representation of given hexadecimal value.

    Examples
    --------
    >>> spark.createDataFrame([('414243',)], ['a']).select(unhex('a')).collect()
    [Row(unhex(a)=bytearray(b'ABC'))]
    """
    raise NotImplementedError


@try_remote_functions
def length(col: "AbstractColOrName") -> Func:
    """Computes the character length of string data or number of bytes of binary data.
    The length of character data includes the trailing spaces. The length of binary data
    includes binary zeros.

    .. versionadded:: 1.5.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        target AbstractCol to work on.

    Returns
    -------
    :class:`~osos.Col`
        length of the value.

    Examples
    --------
    >>> spark.createDataFrame([('ABC ',)], ['a']).select(length('a').alias('length')).collect()
    [Row(length=4)]
    """
    raise NotImplementedError


@try_remote_functions
def octet_length(col: "AbstractColOrName") -> Func:
    """
    Calculates the byte length for the specified string AbstractCol.

    .. versionadded:: 3.3.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        Source AbstractCol or strings

    Returns
    -------
    :class:`~osos.Col`
        Byte length of the col

    Examples
    --------
    >>> from pyspark.sql.functions import octet_length
    >>> spark.createDataFrame([('cat',), ( '\U0001F408',)], ['cat']) \\
    ...      .select(octet_length('cat')).collect()
        [Row(octet_length(cat)=3), Row(octet_length(cat)=4)]
    """
    raise NotImplementedError


@try_remote_functions
def bit_length(col: "AbstractColOrName") -> Func:
    """
    Calculates the bit length for the specified string AbstractCol.

    .. versionadded:: 3.3.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~osos.Col` or str
        Source AbstractCol or strings

    Returns
    -------
    :class:`~osos.Col`
        Bit length of the col

    Examples
    --------
    >>> from pyspark.sql.functions import bit_length
    >>> spark.createDataFrame([('cat',), ( '\U0001F408',)], ['cat']) \\
    ...      .select(bit_length('cat')).collect()
        [Row(bit_length(cat)=24), Row(bit_length(cat)=32)]
    """
    raise NotImplementedError


@try_remote_functions
def translate(srcCol: "AbstractColOrName", matching: str, replace: str) -> Func:
    """A function translate any character in the `srcCol` by a character in `matching`.
    The characters in `replace` is corresponding to the characters in `matching`.
    Translation will happen whenever any character in the string is matching with the character
    in the `matching`.

    .. versionadded:: 1.5.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    srcCol : :class:`~osos.Col` or str
        Source AbstractCol or strings
    matching : str
        matching characters.
    replace : str
        characters for replacement. If this is shorter than `matching` string then
        those chars that don't have replacement will be dropped.

    Returns
    -------
    :class:`~osos.Col`
        replaced value.

    Examples
    --------
    >>> spark.createDataFrame([('translate',)], ['a']).select(translate('a', "rnlt", "123") \\
    ...     .alias('r')).collect()
    [Row(r='1a2s3ae')]
    """
    raise NotImplementedError


def udf(f: Callable) -> Callable:
    # this function returns a function that creates a `Func` node with the name of `udf_func`
    # When the inner function is called with a single argument or an argument list
    # the first (or only) argument is taken
    # to be the name of the column to be operated on.
    # `ArbitraryFunction` is a leaf type that contains the inner udf.
    def inner(*args, **kwargs):
        var = args[0]
        args = args[1:]
        ser = AbstractCol(var)
        wrapped = ArbitraryFunction(f, ())
        arglist = ArgList({"args": args, "kwargs": kwargs}, ())
        return Func(udf_func, ser, wrapped, arglist)

    return inner
