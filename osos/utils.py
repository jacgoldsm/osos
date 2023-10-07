from __future__ import annotations

import itertools
from typing import Iterable, Union,cast,List
from osos.exceptions import OsosTypeError
import pandas as pd
from numbers import Number
from pandas.core.groupby.generic import SeriesGroupBy

from .column import Node, AbstractCol,AbstractLit


SeriesType = Union[pd.Series, SeriesGroupBy]


def flatten_cols(cols: Iterable) -> Iterable:
    # to match PySpark API here, any of the arguments to cols
    # could be a list, which have to be flattened using `chain.from_iterable`.
    # However all the args to `from_iterable` have to be one level nested in lists
    # e.g. ([1,2], [3], [3,4])

    # *HOWEVER*, don't count strings as iterable for this purpose
    if isinstance(cols, tuple):
        cols = list(cols)

    if not isinstance(cols, Iterable) or isinstance(cols, (str, bytes)):
        return [cols]

    def transform_or_noop(col):
        return (
            col
            if isinstance(col, Iterable) and not isinstance(col, (str, bytes))
            else [col]
        )

    cols = [transform_or_noop(col) for col in cols]

    return list(itertools.chain.from_iterable(cols))

def flatten_and_process_cols_or_lits(cols: Iterable[str|Node|Number]) -> Iterable[Node]:
    flat_cols = flatten_cols(cols)
    for i,col in enumerate(flat_cols):
        if isinstance(col, str):
            flat_cols[i] = AbstractCol(col)
        elif isinstance(col, Number):
            flat_cols[i] = AbstractLit(col)
        elif isinstance(col,Node):
            pass
        else:
            raise OsosTypeError(f"Incorrect type: {type(col)}")

    return cast(List[Node],flat_cols)

def flatten_and_process_cols(cols: Iterable[str|Node]) -> Iterable[Node]:
    flat_cols = flatten_cols(cols)
    for i,col in enumerate(flat_cols):
        if isinstance(col, str):
            flat_cols[i] = AbstractCol(col)
        elif isinstance(col,Node):
            pass
        else:
            raise OsosTypeError(f"Incorrect type: {type(col)}")

    return cast(List[Node],flat_cols)

def process_one_col_or_lit(col: str|Node|Number) -> Node:
    if isinstance(col, str):
        col = AbstractCol(col)
    elif isinstance(col, Number):
        col = AbstractLit(col)
    elif isinstance(col,Node):
        pass
    else:
        raise OsosTypeError(f"Incorrect type: {type(col)}")

    return col

def process_one_col(col: str|Node) -> Node:
    if isinstance(col, str):
        col = AbstractCol(col)
    elif isinstance(col,Node):
        pass
    else:
        raise OsosTypeError(f"Incorrect type: {type(col)}")

    return col


def rename_series(series: pd.Series, newname: str, _over=None) -> pd.Series:
    return series.rename(newname)
