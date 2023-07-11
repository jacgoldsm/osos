from .exceptions import AnalysisException
from .utils import flatten_cols
from typing import Union

from .column import Node, AbstractCol, SimpleContainer, ColumnList, ForwardRef
ColumnType = Union[Node, str]



class Window:

    @staticmethod
    def partitionBy(*cols: "ColumnType") -> "WindowSpec":
        cols = flatten_cols(cols)
        for col in cols:
            if isinstance(col, str):
                col = AbstractCol(col)
        return WindowSpec(partition_by=cols)

    @staticmethod
    def orderBy(*cols: "ColumnType") -> "WindowSpec":
        cols = flatten_cols(cols)
        for col in cols:
            if isinstance(col, str):
                col = AbstractCol(col)
        return WindowSpec(partition_by = (), order_by=flatten_cols(cols))

    @staticmethod
    def rowsBetween(start: int, end: int) -> "WindowSpec":
        return WindowSpec(partition_by=(), order_by = (), rows_between = (start,end))

    @staticmethod
    def rangeBetween(start: int, end: int) -> "WindowSpec":
        return WindowSpec(partition_by=(), order_by = (), range_between = (start,end))


def window_getter_func(*args):
    return ConcreteWindowSpec(*args)


class WindowSpec(Node):
    def __init__(
        self,
        partition_by = None,
        order_by = None,
        rows_between = None,
        range_between = None,
    ) -> None:

        partition_by,order_by,rows_between,range_between = (
            () if partition_by is None else partition_by,
            () if order_by is None else order_by,
            () if rows_between is None else rows_between,
            () if range_between is None else range_between,
        )
        
        self._partition_by = ColumnList(partition_by)
        self._order_by = ColumnList(order_by)
        self._rows_between = SimpleContainer("rows_spec", args=rows_between)
        self._range_between = SimpleContainer("range_spec", args=range_between)
        self._args = [self._partition_by, self._order_by, self._rows_between, self._range_between,]
        self._name = window_getter_func
        self._over = ForwardRef("EmptyWindow")

    def __repr__(self):
        return str({
         "Partition": self._partition_by._args,
         "Order": self._order_by._args,
         "RowSpec": self._rows_between._args,
         "RangeSpec": self._range_between._args,
        })

    __str__ = __repr__

    
    def partitionBy(self, *cols: "ColumnType") -> "WindowSpec":
        if self._partition_by._args:
            raise AnalysisException("PartitionBy is already defined")
        cols = flatten_cols(cols)
        for col in cols:
            if isinstance(col, str):
                col = AbstractCol(col)

        cols = ColumnList(cols)
        self._partition_by = cols
        self._args[0] = cols
        return self

    def orderBy(self, *cols: ColumnType) -> "WindowSpec":
        if self._order_by._args:
            raise AnalysisException("orderBy is already defined")
        cols = flatten_cols(cols)
        for col in cols:
            if isinstance(col, str):
                col = AbstractCol(col)

        cols = ColumnList(cols)
        self._order_by = cols
        self._args[1] = cols
        return self

    def rowsBetween(self, start: int, end: int) -> "WindowSpec":

        if self._rows_between._args:
            raise AnalysisException("rowsBetween is already defined")
        if self._range_between._args:
            raise AnalysisException("Cannot define both range spec and rows spec")
        spec = SimpleContainer("rows_spec", args = ((start,end,),))
        self._rows_between = spec
        self._args[2] = spec
        return self

    def rangeBetween(self, start: int, end: int) -> "WindowSpec":

        if self._range_between._args:
            raise AnalysisException("rangeBetween is already defined")
        if self._rows_between._args:
            raise AnalysisException("Cannot define both range spec and rows spec")
        spec = SimpleContainer("range_spec", args = ((start,end,),))
        self._rows_between = spec
        self._args[3] = spec
        return self

class ConcreteWindowSpec:
    def __init__(
        self,
        partition_by = None,
        order_by = None,
        rows_between = None,
        range_between = None,
    ) -> None:
        self.partition_by: list["ColumnType"] = partition_by
        self.order_by: list["ColumnType"] = order_by
        self.rows_between: "SimpleContainer" = rows_between
        self.range_between: "SimpleContainer" = range_between


class EmptyWindow(WindowSpec):

    def __str__(self): 
        return "<EmptyWindow>"

    __repr__ = __str__


# COPIED FROM SPARK TO RETAIN SEMANTICS
# MAX_WINDOW_SIZE is the number of rows between unboundedPreceding and unboundedFollowing
import sys
_JAVA_MIN_LONG = -(1 << 63)  # -9223372036854775808
_JAVA_MAX_LONG = (1 << 63) - 1  # 9223372036854775807
_PRECEDING_THRESHOLD = max(-sys.maxsize, _JAVA_MIN_LONG)
_FOLLOWING_THRESHOLD = min(sys.maxsize, _JAVA_MAX_LONG)

unboundedPreceding: int = _JAVA_MIN_LONG

unboundedFollowing: int = _JAVA_MAX_LONG
MAX_WINDOW_SIZE: int = unboundedFollowing - unboundedPreceding

currentRow: int = 0