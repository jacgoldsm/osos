import itertools
from typing import Iterable, Union,cast,List
import pandas as pd
from pandas.core.groupby.generic import SeriesGroupBy

from .column import Node, AbstractCol


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

def flatten_and_process(cols: Iterable[Union[str,Node]]) -> Iterable[Node]:
    flat_cols = flatten_cols(cols)
    for i,col in enumerate(flat_cols):
        if isinstance(col, str):
            flat_cols[i] = AbstractCol(col)

    return cast(List[Node],flat_cols)


def rename_series(series: pd.Series, newname: str, _over=None) -> pd.Series:
    return series.rename(newname)
