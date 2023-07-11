import numpy as np

from functions import col
from dataframe import DataFrame
import functions as F
from window import Window

if __name__ == '__main__':
    data = {
        'foo':list(range(10)),
        'baz':list(reversed(range(10))),
        'tup':['foo' for _ in range(5)] + ['bar' for _ in range(5)],

    }

    df2 = DataFrame.fromDict(data)
    #df = df2.select("foo", F.col("baz"))

    j = DataFrame({'col':[np.nan, 'foo', 'bar', 'baz'], 'val':[1,2,3,4]})
    k = DataFrame({'col':[np.nan, 'foo', 'zoo', 'baz'], 'var':[5,6,7,8]})
    l = DataFrame({'col':['foo', 'foo', 'zoo', 'zoo'], 'var':[5,6,7,8]})



    #times_two = F.udf(lambda x: x * 2)

    # print(df.withColumn("boo", col("foo") + col("baz")))
    # print(df.withColumn("boo", col("foo") * col("baz")))
    # print(df.withColumn("boo", col("foo") / col("baz")))
    # print(df.withColumn("boo", col("foo") - col("baz")))
    # print(df.withColumn("boo", ~col("foo")))
    # print(df.agg(F.sum("baz")))
    # print(df.select(F.col("foo").alias("moo")))
    # print(df2.groupBy('tup').agg(F.sum("baz")))
    # print(DataFrame.fromPandas(df._data))
    # print(df.withColumnRenamed("foo", "jk"))
    # print(j.join(k, 'col', 'inner'))
    # print("\n")
    # print(j.join(k, 'col', 'left'))
    # print("\n")
    # print(j.join(k, 'col', 'outer'))
    # print("\n")
    # print(j.join(k, 'col', 'right'))
    #print(df.withColumn("moo", times_two('baz')))
    #print(F.sum("var").over(Window.partitionBy("col")))
    print(l.withColumn("boo", F.sum("var").over(Window.partitionBy("col"))))
