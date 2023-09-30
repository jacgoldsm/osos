import pandas as pd
from textwrap import dedent
import warnings
import re
from typing import Optional

from osos.dataframe import DataFrame
from osos.exceptions import AnalysisException


def createDataFrame(data, schema=None) -> DataFrame:
    dedent(
        """Compatibility with Spark, though this is not the recommended way of creating a dataframe.
    Schemas will be ignored except for the column names. Data must be a pandas dataframe, tuple, or list, 
    schema has to be a list or a simple string"""
    )
    schema = _parse_schema(schema)
    if isinstance(data, (dict,tuple)):
        data = [data]
    
    if isinstance(data, pd.DataFrame):
        return DataFrame(data)

    if isinstance(data[0], dict):
        warnings.warn("This is the Spark way of defining a DataFrame, not the Pandas way. Each "
                     "dict represents a row, not the whole data")

    if isinstance(data, list):
        # either a list of tuples or a list of dicts.
        # either way, handled by pd.DataFrame.from_records
        return DataFrame(pd.DataFrame.from_records(data, columns = schema))
    else:
        raise AnalysisException("Data must be a dict, tuple, pandas DataFrame, list[dict], or list[tuple]")

def _parse_schema(schema):
    if isinstance(schema, str):
        # we have a simple string like "a: int, b: str"
        # or just "a b c d"
        if ":" not in schema:
            return schema.split()
        schema = "".join(schema.split()).split(",")
        cols = []
        for elem in schema:
            cols.append(re.search('(.*):', elem).group(1))
        return cols
    elif isinstance(schema,list):
        return schema
    else:
        raise TypeError("schema must be str or list")


def range(start: int, end: Optional[int] = None, step: int = 1, numSlices: Optional[int] = None):
    import numpy as np
    if end is None:
        end = start
        start = 0

    return DataFrame(pd.DataFrame({"id":np.arange(start,end,step)}))


class read:
    def csv(path, *args, **kwargs):
        return pd.read_csv(path, *args, **kwargs)

    def text(path, *args, **kwargs):
        return pd.read_csv(path, *args, **kwargs)

def _test():
    pd_data = pd.DataFrame({"a":[1,2,3], "b":[4,5,6]})
    tuple_data = (1,2)
    dict_data = {"a":1, "b":2}
    list_tuple_data = [(1,2), (3,4)]
    list_dict_data = [{"a":1, "b":2}, {"a":3, "b":4}]
    str_schema_one = "ai bi"
    str_schema_two = "ai: int, bi:int"
    list_schema = ["ai", "bi"]

    print(_parse_schema(str_schema_one))
    print()
    print(_parse_schema(str_schema_two))
    print()
    print(_parse_schema(list_schema))
    print()
    print(createDataFrame(pd_data))
    print()
    print(createDataFrame(pd_data,str_schema_one))
    print()
    print(createDataFrame(tuple_data))
    print()
    print(createDataFrame(tuple_data,str_schema_two))
    print()
    print(createDataFrame(list_tuple_data, str_schema_one))
    print()
    print(createDataFrame(list_dict_data))
    print()
    print(createDataFrame(dict_data))


if __name__ == '__main__':
    _test()


