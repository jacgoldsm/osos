from pandas.api.indexers import BaseIndexer
import pandas as pd
import numpy as np
from typing import Union


from .window import currentRow,unboundedFollowing,unboundedPreceding

class SparkIndexer(BaseIndexer):
    
    def __init__(
        self,
        series: pd.Series,
        row_start: int = unboundedPreceding,
        row_end: int = currentRow,
        row_or_range = 'row',
        range_col = None,
    ):
        self.series = series
        self.row_start = row_start
        self.row_end = row_end
        self.row_or_range = row_or_range
        self.range_col = range_col
        super().__init__(self)

    def get_window_bounds(
        self, 
        num_values: int = None, 
        min_periods: Union[int,None] = None, 
        center: Union[bool,None] = None,
        closed: Union[str,None] = None
    ) -> tuple[np.ndarray, np.ndarray]:

        # case 1: partitioned by column(s), no range/row spec
        # the partitioning is already done by the groupby,
        # so we return an unbounded range spec
        if self.row_start is None:
            return (
            np.full(self.series,unboundedPreceding,np.int64),
            np.full(self.series,unboundedFollowing,np.int64)
            )

        # case 2: `rowsBetween` is used. It doesn't matter if the series
        # is partitioned/ordered because ordering/partitioning has already been done
        if self.row_or_range == 'row':
            return (
                self.series.index + self.row_start,
                self.series.index + self.row_end
            )

        # case 3: `rangeBetween` is used. `rangeBetween` specifically makes
        # reference to the `orderBy` col. This is going to be incredibly inefficient ;(
        if self.row_or_range == 'range':
            start = np.empty(self.series,dtype=np.int64)
            end = np.empty(self.series,dtype=np.int64)
            length = len(self.series)

            start_finished,end_finished = False,False
            for i in range(length):
                for j in range(length):
                    if (self.range_col[i] - self.range_col[j] < self.row_start) and not start_finished:
                        start[i] = j
                        start_finished = True
                    if (self.range_col[i] - self.range_col[j] > self.row_end) and not end_finished:
                        end[i] = j
                        end_finished = True

                    if end_finished and start_finished:
                        break

            return start,end
                    

