import numpy as np
import pandas as pd
from decimal import Decimal

from typing import List,Tuple

# integral
IntegerType = lambda: np.int32
FloatType = lambda: np.float32
ByteType = lambda: np.int8
ShortType = lambda: np.int16
LongType = lambda: np.int64
DoubleType = lambda: np.float64
DecimalType = lambda: Decimal

# string
StringType = lambda: str
VarcharType = lambda length: str
CharType = lambda length: str

# binary
BinaryType = lambda: bytes

# boolean
BooleanType = lambda: np.bool8

# datetime
DateType = lambda: np.datetime64
TimestampType = lambda: pd.Timestamp
TimestampNTZType = lambda: pd.Timestamp

# interval
YearMonthIntervalType = lambda *args: pd.Timedelta
DayTimeIntervalType = lambda *args: pd.Timedelta

# complex
ArrayType = lambda elementType, containsNull=False: List
MapType = lambda keyType, elementType, valueContainsNull=False: dict
StructType = lambda fields: np.array
StructField = lambda name,dataType,nullable=False: Tuple




