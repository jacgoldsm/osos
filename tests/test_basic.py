import osos.functions as F
from osos.dataframe import DataFrame
from osos.window import Window
from osos.functions import col


import numpy as np
import pandas as pd


data = {
        'foo':list(range(10)),
        'baz':list(reversed(range(10))),
        'tup':['foo' for _ in range(5)] + ['bar' for _ in range(5)],

}

one = DataFrame.fromDict(data)
two = one.select("foo", F.col("baz"))

three = DataFrame({'col':[np.nan, 'foo', 'bar', 'baz'], 'val':[1,2,3,4]})
four = DataFrame({'col':[np.nan, 'foo', 'zoo', 'baz'], 'var':[5,6,7,8]})
five = DataFrame({'col':['foo', 'foo', 'zoo', 'zoo'], 'var':[5,6,7,8]})



times_two = F.udf(lambda x: x * 2)

a = one.withColumn("boo", col("foo") + col("baz"))
b = one.withColumn("boo", col("foo") * col("baz"))
c = one.withColumn("boo", col("foo") / col("baz"))
d = one.withColumn("boo", col("foo") - col("baz"))
e = one.withColumn("boo", ~col("foo"))
f = one.agg(F.sum("baz"))
g = one.select(F.col("foo").alias("moo"))
h = one.groupBy('tup').agg(F.sum("baz"))
i = one.withColumnRenamed("foo", "jk")
j = three.join(four, 'col', 'inner')
k = three.join(four, 'col', 'left')
l = three.join(four, 'col', 'outer')
m = three.join(four, 'col', 'right')
n = one.withColumn("moo", times_two('baz'))
o = five.withColumn("boo", F.sum("var").over(Window.partitionBy("col")))

ap = one._data.assign(boo=one._data['foo'] + one._data['baz'])



def compares_equal(osos_dataframe: DataFrame,pandas_dataframe: pd.DataFrame) -> bool:
    return osos_dataframe.toPandas().equals(pandas_dataframe)



def test_all():
    assert compares_equal(a,ap)