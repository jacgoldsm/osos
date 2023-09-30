import osos.functions as F
from osos.dataframe import DataFrame
from osos.window import Window
from osos.functions import col
from osos import OsosSession


import numpy as np
import pandas as pd

from pandas.testing import assert_frame_equal


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
six["var"] = six["var"].sort_values(ascending=False)
six = DataFrame(six)
seven = DataFrame.fromDict(data)
seven._data["eggs"] = pd.Series([1, 1, 2, 2, 3, 3, 4, 4, 5, 5])


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
q = one.filter(F.col("tup") == "foo")
r = one.withColumn("rn", F.row_number().over(Window.partitionBy("tup").orderBy("baz")))
s = seven.withColumn(
    "rn", F.row_number().over(Window.partitionBy("tup", "eggs").orderBy("baz"))
)
t = seven.withColumn(
    "rn", F.row_number().over(Window.partitionBy("tup").orderBy("eggs", "baz"))
)
u = one.withColumn("foosqrt", F.sqrt("foo"))
v = one.agg(F.median("baz").alias("baz"))
w = one.withColumn("tup", F.upper("tup"))
df = OsosSession.range(3)
x = df.select(F.when(df['id'] == 2, 3).otherwise(4).alias("age"))



ap = one._data.assign(boo=one._data["foo"] + one._data["baz"])
bp = one._data.assign(boo=one._data["foo"] * one._data["baz"])
cp = one._data.assign(boo=one._data["foo"] / one._data["baz"])
dp = one._data.assign(boo=one._data["foo"] - one._data["baz"])
ep = one._data.assign(boo=~one._data["foo"])
fp = pd.DataFrame(one._data.agg({"baz": sum})).T
gp = one._data[["foo"]].rename({"foo": "moo"}, axis="columns")
hp = one._data.groupby("tup").agg({"baz": sum}).reset_index()
ip = one._data.rename({"foo": "jk"}, axis="columns")
jp = three._data.dropna(subset=["col"]).merge(
    four._data.dropna(subset=["col"]), on=["col"], how="inner"
)
kp = three._data.merge(four._data.dropna(subset=["col"]), on=["col"], how="left")
lp_notnull = three._data.dropna(subset=["col"]).merge(
    four._data.dropna(subset=["col"]), on=["col"], how="outer"
)
lp_leftnull = three._data[three._data.isnull().any(axis=1)]
lp_rightnull = four._data[four._data.isnull().any(axis=1)]
lp_allnull = pd.concat([lp_leftnull, lp_rightnull], axis=0, ignore_index=True)
lp = pd.concat([lp_notnull, lp_allnull], axis=0, ignore_index=True)
mp = three._data.dropna(subset=["col"]).merge(four._data, on=["col"], how="right")
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
rp = one._data.assign(
    **{"rn": list(reversed(range(1, 6))) + list(reversed(range(1, 6)))}
)
sp = seven._data.assign(**{"rn": [2, 1, 2, 1, 1, 1, 2, 1, 2, 1]})
tp = seven._data.assign(**{"rn": [2, 1, 4, 3, 5, 1, 3, 2, 5, 4]})
up = one._data.assign(**{"foosqrt": np.sqrt(one._data.foo)})
vp = pd.DataFrame(one._data.agg({"baz": np.median})).T
wp = one._data.assign(tup=one._data["tup"].str.upper())
xp = pd.DataFrame({'age':[4,4,3]})


def compares_equal(osos_dataframe: DataFrame, pandas_dataframe: pd.DataFrame) -> bool:
    try:
        assert_frame_equal(osos_dataframe.toPandas(), pandas_dataframe)
        return True
    except AssertionError:
        return False


def test_methods():
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
    assert compares_equal(v, vp)
    assert compares_equal(w, wp)


def test_functions():
    assert compares_equal(r, rp)
    assert compares_equal(s, sp)
    assert compares_equal(t, tp)
    assert compares_equal(u, up)
    assert compares_equal(x,xp)


iris_pd = pd.read_csv(
    "https://raw.githubusercontent.com/mwaskom/seaborn-data/master/iris.csv"
)
# Make an osos DataFrame
iris = DataFrame(iris_pd)
# manipulate it just like Pyspark
iris_pd = iris_pd.copy()
agg = (
    iris.withColumn("sepal_area", F.col("sepal_length") * F.col("sepal_width"))
    .filter(F.col("sepal_length") > 4.9)
    .withColumn(
        "total_area_by_species", F.sum("sepal_area").over(Window.partitionBy("species"))
    )
    .withColumn("species", F.lower("species"))
    .groupBy("species")
    .agg(
        F.avg("sepal_length").alias("avg_sepal_length"),
        F.avg("sepal_area").alias("avg_sepal_area"),
    )
)

iris_pd["sepal_area"] = iris_pd["sepal_length"] * iris_pd["sepal_width"]
iris_pd = iris_pd[iris_pd["sepal_length"] > 4.9]
iris_pd["total_area_by_species"] = np.array(
    iris_pd.groupby("species")
    .rolling(1000, center=True, min_periods=1)
    .sum()["sepal_area"]
    .astype(np.float64)
)
iris_pd["species"] = iris_pd["species"].str.lower()
iris_pd = (
    iris_pd.groupby("species")
    .agg(lambda x: x.mean())
    .reset_index()[["species", "sepal_length", "sepal_area"]]
)
iris_pd = iris_pd.rename(
    columns={"sepal_length": "avg_sepal_length", "sepal_area": "avg_sepal_area"}
)


def test_iris():
    assert compares_equal(agg, iris_pd)
