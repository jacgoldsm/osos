import osos.functions as F
from osos.dataframe import DataFrame
from osos.window import Window
from osos.functions import col


import numpy as np
import pandas as pd


data = {
    "foo": list(range(10)),
    "baz": list(reversed(range(10))),
    "tup": ["foo" for _ in range(5)] + ["bar" for _ in range(5)],
}

one = DataFrame.fromDict(data)
two = one.select("foo", F.col("baz"))

three = DataFrame({"col": [np.nan, "foo", "bar", "baz"], "val": [1, 2, 3, 4]})
four = DataFrame({"col": [np.nan, "foo", "zoo", "baz"], "var": [5, 6, 7, 8]})
five = DataFrame({"col": ["foo", "foo", "zoo", "zoo"], "var": [5, 6, 7, 8]})
six = five._data.copy()
six['var'] = six['var'].sort_values(ascending=False)
six = DataFrame(six)


times_two = F.udf(lambda x: x * 2)

a = one.withColumn("boo", col("foo") + col("baz"))
b = one.withColumn("boo", col("foo") * col("baz"))
c = one.withColumn("boo", col("foo") / col("baz"))
d = one.withColumn("boo", col("foo") - col("baz"))
e = one.withColumn("boo", ~col("foo"))
f = one.agg(F.sum("baz").alias("baz"))
g = one.select(F.col("foo").alias("moo"))
h = one.groupBy("tup").agg(F.sum("baz"))
i = one.withColumnRenamed("foo", "jk")
j = three.join(four, "col", "inner")
k = three.join(four, "col", "left")
l = three.join(four, "col", "outer")
m = three.join(four, "col", "right")
n = one.withColumn("moo", times_two("baz"))
o = five.withColumn("boo", F.sum("var").over(Window.partitionBy("col")))
p = six.withColumn("boo", F.sum("var").over(Window.partitionBy("col").orderBy("var")))
print(p)
q = one.filter(F.col("tup") == "foo")

ap = one._data.assign(boo=one._data["foo"] + one._data["baz"])
bp = one._data.assign(boo=one._data["foo"] * one._data["baz"])
cp = one._data.assign(boo=one._data["foo"] / one._data["baz"])
dp = one._data.assign(boo=one._data["foo"] - one._data["baz"])
ep = one._data.assign(boo=~one._data["foo"])
fp = pd.DataFrame(one._data.agg({"baz": sum})).T
gp = one._data[["foo"]].rename({"foo": "moo"}, axis="columns")
hp = one._data.groupby("tup").agg({"baz": sum})
ip = one._data.rename({"foo": "jk"}, axis="columns")
jp = three._data.dropna(subset="col").merge(
    four._data.dropna(subset="col"), on=["col"], how="inner"
)
kp = three._data.merge(four._data.dropna(subset="col"), on=["col"], how="left")
lp_notnull = three._data.dropna(subset="col").merge(
    four._data.dropna(subset="col"), on=["col"], how="outer"
)
lp_leftnull = three._data[three._data.isnull().any(axis=1)]
lp_rightnull = four._data[four._data.isnull().any(axis=1)]
lp_allnull = pd.concat([lp_leftnull, lp_rightnull], axis=0, ignore_index=True)
lp = pd.concat([lp_notnull, lp_allnull], axis=0, ignore_index=True)
mp = three._data.dropna(subset="col").merge(four._data, on=["col"], how="right")
np_ = one._data.assign(moo=one._data["baz"] * 2)
# this is incredibly annoying to do in Pandas. Motivation for Osos!
calc = np.array(
    five._data.groupby("col")
    .rolling(100, min_periods=0, center=True)
    .sum()["var"]
    .astype(int)
)
op = five._data.copy()
op["boo"] = calc
calc = np.array(
    five._data.groupby("col")
    .rolling(100, min_periods=0, center=False)
    .sum()["var"]
    .astype(int)
)
pp = five._data.copy()
pp["boo"] = calc
qp = one._data[one._data["tup"] == "foo"]


def compares_equal(osos_dataframe: DataFrame, pandas_dataframe: pd.DataFrame) -> bool:
    return osos_dataframe.toPandas().equals(pandas_dataframe)


def test_all():
    assert compares_equal(a, ap)
    assert compares_equal(b, bp)
    assert compares_equal(c, cp)
    assert compares_equal(d, dp)
    assert compares_equal(e, ep)
    assert compares_equal(f, fp)
    assert compares_equal(g, gp)
    assert compares_equal(h, hp)
    assert compares_equal(i, ip)
    assert compares_equal(j, jp)
    assert compares_equal(k, kp)
    assert compares_equal(l, lp)
    assert compares_equal(m, mp)
    assert compares_equal(n, np_)
    assert compares_equal(o, op)
    assert compares_equal(p, pp)
    assert compares_equal(q, qp)
