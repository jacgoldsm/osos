from numbers import Number
from typing import Union, Iterable, TYPE_CHECKING,List
import operator
from copy import deepcopy
import numpy as np
import pandas as pd


if TYPE_CHECKING:
    from .window import WindowSpec, EmptyWindow


NumOrCol = Union[Number, "AbstractColOrLit"]

def rename_series(series, newname: str, _over=None) -> pd.Series:
    return series.rename(newname)

class Node:
    def __init__(self, name, args):
        self._name = name
        self._args = args
        self._over = ForwardRef("EmptyWindow")

    def __str__(root, markerStr="+- ", levelMarkers=None):
        levelMarkers = levelMarkers if levelMarkers is not None else []
        emptyStr = " " * len(markerStr)
        connectionStr = "|" + emptyStr[:-1]
        level = len(levelMarkers)
        mapper = lambda draw: connectionStr if draw else emptyStr
        markers = "".join(map(mapper, levelMarkers[:-1]))
        markers += markerStr if level > 0 else ""
        out = f"{markers}{root._name}\n"
        for i, child in enumerate(root._args):
            isLast = i == len(root._args) - 1
            out += Node.__str__(child, markerStr, [*levelMarkers, not isLast])

        if hasattr(root, "_over"):
            out += "Over: " + str(root._over)

        return out

    def __add__(self, other: NumOrCol):
        other = AbstractLit(other) if not isinstance(other, Node) else other
        return BinaryOp(operator.add, self, other)

    __radd__ = __add__

    def __mul__(self, other: NumOrCol):
        other = AbstractLit(other) if not isinstance(other, Node) else other
        return BinaryOp(operator.mul, self, other)

    __rmul__ = __mul__

    def __truediv__(self, other: NumOrCol):
        other = AbstractLit(other) if not isinstance(other, Node) else other
        return BinaryOp(operator.truediv, self, other)

    __rtruediv__ = __truediv__

    def __sub__(self, other: NumOrCol):
        other = AbstractLit(other) if not isinstance(other, Node) else other
        return BinaryOp(operator.sub, self, other)

    __rsub__ = __sub__

    def __invert__(self):
        return UnaryOp(operator.__inv__, self)

    def __and__(self, other):
        other = AbstractLit(other) if not isinstance(other, Node) else other
        return BinaryOp(operator.__and__, self, other)

    def __or__(self, other):
        other = AbstractLit(other) if not isinstance(other, Node) else other
        return BinaryOp(operator.__or__, self, other)

    def __eq__(self, other):
        other = AbstractLit(other) if not isinstance(other, Node) else other
        return BinaryOp(operator.__eq__, self, other)

    def __gt__(self, other):
        other = AbstractLit(other) if not isinstance(other, Node) else other
        return BinaryOp(operator.__gt__, self, other)

    def __lt__(self, other):
        other = AbstractLit(other) if not isinstance(other, Node) else other
        return BinaryOp(operator.__lt__, self, other)

    def __ne__(self, other):
        other = AbstractLit(other) if not isinstance(other, Node) else other
        return BinaryOp(operator.__ne__, self, other)

    def alias(self, newname):
        return Func(rename_series, self, NameString(newname, ()))


class ColumnList(Node):
    def __init__(self, args: List["AbstractColOrLit"]):
        def extract_columns(*args):
            return list(args)

        self._name = extract_columns
        self._args = [AbstractCol(arg) for arg in args]
        self._over = ForwardRef("EmptyWindow")

    def __bool__(self):
        return bool(self._args)


class FuncOrOp(Node):
    pass


class ForwardRef:
    def __init__(self, reference: str, args: Union[List, None] = None):
        self.reference = reference
        self.args = args if args is not None else []


class Func(FuncOrOp):
    def __init__(self, name, *args, over=ForwardRef("EmptyWindow")):

        self._name = name
        self._args = args
        self._over = over

    def over(self, partition: "WindowSpec"):
        return self.__class__(
            self._name,
            *self._args,
            over=partition,
        )


class FuncWithNoArgs(Func):
    pass


class Op(FuncOrOp):
    pass


class UnaryOp(Op):
    def __init__(self, name, arg):
        self._name = name
        self._args = (arg,)


class BinaryOp(Op):
    def __init__(self, name, lvalue, rvalue):
        self._name = name
        self._rvalue = rvalue
        self._lvalue = lvalue
        self._args = (
            lvalue,
            rvalue,
        )
        self._over = ForwardRef("EmptyWindow")


class AbstractColOrLit(Node):
    def __init__(self, name):
        self._name = name
        self._args = ()

    def __str__(self):
        return f"""<Column Name>: {self._name}"""


class AbstractCol(AbstractColOrLit):
    pass


class AbstractLit(AbstractColOrLit):
    pass


class AbstractIndex(Node):
    def __init__(self):
        self._name = ".index"
        self._args = ()

    def __str__(self):
        return "<Index>"


class SimpleContainer(Node):
    def __bool__(self):
        return bool(self._args)

    def __str__(self):
        return str(self._args)

    __repr__ = __str__


class NameString(SimpleContainer):
    pass


class ArbitraryFunction(SimpleContainer):
    pass


class ArgList(SimpleContainer):
    pass


AbstractColOrName = Union[AbstractCol, str]


def make_series_from_literal(value, length):
    return pd.Series(np.full(length, value))
