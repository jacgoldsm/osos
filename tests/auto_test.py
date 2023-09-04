#pyspark.sql.functions.col:
col('x')
column('x')

#pyspark.sql.functions.column:
col('x')
column('x')

#pyspark.sql.functions.lit:
df = spark.range(1)
df.select(lit(5).alias('height'), df.id).show()

spark.range(1).select(lit([1, 2, 3])).show()

#pyspark.sql.functions.broadcast:
from pyspark.sql import types
df = spark.createDataFrame([1, 2, 3, 3, 4], types.IntegerType())
df_small = spark.range(3)
df_b = broadcast(df_small)
df.join(df_b, df.value == df_small.id).show()

#pyspark.sql.functions.coalesce:
cDf = spark.createDataFrame([(None, None), (1, None), (None, 2)], ("a", "b"))
cDf.show()

cDf.select(coalesce(cDf["a"], cDf["b"])).show()

cDf.select('*', coalesce(cDf["a"], lit(0.0))).show()

#pyspark.sql.functions.input_file_name:
import os
path = os.path.abspath(__file__)
df = spark.read.text(path)
df.select(input_file_name()).first()

#pyspark.sql.functions.isnan:
df = spark.createDataFrame([(1.0, float('nan')), (float('nan'), 2.0)], ("a", "b"))
df.select("a", "b", isnan("a").alias("r1"), isnan(df.b).alias("r2")).show()

#pyspark.sql.functions.isnull:
df = spark.createDataFrame([(1, None), (None, 2)], ("a", "b"))
df.select("a", "b", isnull("a").alias("r1"), isnull(df.b).alias("r2")).show()

#pyspark.sql.functions.monotonically_increasing_id:
df0 = sc.parallelize(range(2), 2).mapPartitions(lambda x: [(1,), (2,), (3,)]).toDF(['col1'])
df0.select(monotonically_increasing_id().alias('id')).collect()

#pyspark.sql.functions.nanvl:
df = spark.createDataFrame([(1.0, float('nan')), (float('nan'), 2.0)], ("a", "b"))
df.select(nanvl("a", "b").alias("r1"), nanvl(df.a, df.b).alias("r2")).collect()

#pyspark.sql.functions.rand:
df = spark.range(2)
df.withColumn('rand', rand(seed=42) * 3).show()

#pyspark.sql.functions.randn:
df = spark.range(2)
df.withColumn('randn', randn(seed=42)).show()

#pyspark.sql.functions.spark_partition_id:
df = spark.range(2)
df.repartition(1).select(spark_partition_id().alias("pid")).collect()

#pyspark.sql.functions.when:
df = spark.range(3)
df.select(when(df['id'] == 2, 3).otherwise(4).alias("age")).show()

df.select(when(df.id == 2, df.id + 1).alias("age")).show()

#pyspark.sql.functions.bitwise_not:
df = spark.range(1)
df.select(bitwise_not(lit(0))).show()
df.select(bitwise_not(lit(1))).show()

#pyspark.sql.functions.bitwiseNOT:
#pyspark.sql.functions.expr:
df = spark.createDataFrame([["Alice"], ["Bob"]], ["name"])
df.select("name", expr("length(name)")).show()

#pyspark.sql.functions.greatest:
df = spark.createDataFrame([(1, 4, 3)], ['a', 'b', 'c'])
df.select(greatest(df.a, df.b, df.c).alias("greatest")).collect()

#pyspark.sql.functions.least:
df = spark.createDataFrame([(1, 4, 3)], ['a', 'b', 'c'])
df.select(least(df.a, df.b, df.c).alias("least")).collect()

#pyspark.sql.functions.sqrt:
df = spark.range(1)
df.select(sqrt(lit(4))).show()

#pyspark.sql.functions.abs:
df = spark.range(1)
df.select(abs(lit(-1))).show()

#pyspark.sql.functions.acos:
df = spark.range(1, 3)
df.select(acos(df.id)).show()

#pyspark.sql.functions.acosh:
df = spark.range(2)
df.select(acosh(col("id"))).show()

#pyspark.sql.functions.asin:
df = spark.createDataFrame([(0,), (2,)])
df.select(asin(df.schema.fieldNames()[0])).show()

#pyspark.sql.functions.asinh:
df = spark.range(1)
df.select(asinh(col("id"))).show()

#pyspark.sql.functions.atan:
df = spark.range(1)
df.select(atan(df.id)).show()

#pyspark.sql.functions.atanh:
df = spark.createDataFrame([(0,), (2,)], schema=["numbers"])
df.select(atanh(df["numbers"])).show()

#pyspark.sql.functions.atan2:
df = spark.range(1)
df.select(atan2(lit(1), lit(2))).first()

#pyspark.sql.functions.bin:
df = spark.createDataFrame([2,5], "INT")
df.select(bin(df.value).alias('c')).collect()

#pyspark.sql.functions.cbrt:
df = spark.range(1)
df.select(cbrt(lit(27))).show()

#pyspark.sql.functions.ceil:
df = spark.range(1)
df.select(ceil(lit(-0.1))).show()

#pyspark.sql.functions.conv:
df = spark.createDataFrame([("010101",)], ['n'])
df.select(conv(df.n, 2, 16).alias('hex')).collect()

#pyspark.sql.functions.cos:
import math
df = spark.range(1)
df.select(cos(lit(math.pi))).first()

#pyspark.sql.functions.cosh:
df = spark.range(1)
df.select(cosh(lit(1))).first()

#pyspark.sql.functions.cot:
import math
df = spark.range(1)
df.select(cot(lit(math.radians(45)))).first()

#pyspark.sql.functions.csc:
import math
df = spark.range(1)
df.select(csc(lit(math.radians(90)))).first()

#pyspark.sql.functions.exp:
df = spark.range(1)
df.select(exp(lit(0))).show()

#pyspark.sql.functions.expm1:
df = spark.range(1)
df.select(expm1(lit(1))).first()

#pyspark.sql.functions.factorial:
df = spark.createDataFrame([(5,)], ['n'])
df.select(factorial(df.n).alias('f')).collect()

#pyspark.sql.functions.floor:
df = spark.range(1)
df.select(floor(lit(2.5))).show()

#pyspark.sql.functions.hex:
spark.createDataFrame([('ABC', 3)], ['a', 'b']).select(hex('a'), hex('b')).collect()

#pyspark.sql.functions.unhex:
spark.createDataFrame([('414243',)], ['a']).select(unhex('a')).collect()

#pyspark.sql.functions.hypot:
df = spark.range(1)
df.select(hypot(lit(1), lit(2))).first()

#pyspark.sql.functions.log:
df = spark.createDataFrame([10, 100, 1000], "INT")
df.select(log(10.0, df.value).alias('ten')).show()

df.select(log(df.value)).show()

#pyspark.sql.functions.log10:
df = spark.range(1)
df.select(log10(lit(100))).show()

#pyspark.sql.functions.log1p:
import math
df = spark.range(1)
df.select(log1p(lit(math.e))).first()

df.select(log(lit(math.e+1))).first()

#pyspark.sql.functions.log2:
df = spark.createDataFrame([(4,)], ['a'])
df.select(log2('a').alias('log2')).show()

#pyspark.sql.functions.pmod:
from pyspark.sql.functions import pmod
df = spark.createDataFrame([
(1.0, float('nan')), (float('nan'), 2.0), (10.0, 3.0),
(float('nan'), float('nan')), (-3.0, 4.0), (-10.0, 3.0),
(-5.0, -6.0), (7.0, -8.0), (1.0, 2.0)],
("a", "b"))
df.select(pmod("a", "b")).show()

#pyspark.sql.functions.pow:
df = spark.range(1)
df.select(pow(lit(3), lit(2))).first()

#pyspark.sql.functions.rint:
df = spark.range(1)
df.select(rint(lit(10.6))).show()

df.select(rint(lit(10.3))).show()

#pyspark.sql.functions.round:
spark.createDataFrame([(2.5,)], ['a']).select(round('a', 0).alias('r')).collect()

#pyspark.sql.functions.bround:
spark.createDataFrame([(2.5,)], ['a']).select(bround('a', 0).alias('r')).collect()

#pyspark.sql.functions.sec:
df = spark.range(1)
df.select(sec(lit(1.5))).first()

#pyspark.sql.functions.shiftleft:
spark.createDataFrame([(21,)], ['a']).select(shiftleft('a', 1).alias('r')).collect()

#pyspark.sql.functions.shiftright:
spark.createDataFrame([(42,)], ['a']).select(shiftright('a', 1).alias('r')).collect()

#pyspark.sql.functions.shiftrightunsigned:
df = spark.createDataFrame([(-42,)], ['a'])
df.select(shiftrightunsigned('a', 1).alias('r')).collect()

#pyspark.sql.functions.signum:
df = spark.range(1)
df.select(signum(lit(-5))).show()

df.select(signum(lit(6))).show()

#pyspark.sql.functions.sin:
import math
df = spark.range(1)
df.select(sin(lit(math.radians(90)))).first()

#pyspark.sql.functions.sinh:
df = spark.range(1)
df.select(sinh(lit(1.1))).first()

#pyspark.sql.functions.tan:
import math
df = spark.range(1)
df.select(tan(lit(math.radians(45)))).first()

#pyspark.sql.functions.tanh:
import math
df = spark.range(1)
df.select(tanh(lit(math.radians(90)))).first()

#pyspark.sql.functions.toDegrees:
#pyspark.sql.functions.degrees:
import math
df = spark.range(1)
df.select(degrees(lit(math.pi))).first()

#pyspark.sql.functions.toRadians:
#pyspark.sql.functions.radians:
df = spark.range(1)
df.select(radians(lit(180))).first()

#pyspark.sql.functions.add_months:
df = spark.createDataFrame([('2015-04-08', 2)], ['dt', 'add'])
df.select(add_months(df.dt, 1).alias('next_month')).collect()
df.select(add_months(df.dt, df.add.cast('integer')).alias('next_month')).collect()
df.select(add_months('dt', -2).alias('prev_month')).collect()

#pyspark.sql.functions.current_date:
df = spark.range(1)
df.select(current_date()).show()

#pyspark.sql.functions.current_timestamp:
df = spark.range(1)
df.select(current_timestamp()).show(truncate=False)

#pyspark.sql.functions.date_add:
df = spark.createDataFrame([('2015-04-08', 2,)], ['dt', 'add'])
df.select(date_add(df.dt, 1).alias('next_date')).collect()
df.select(date_add(df.dt, df.add.cast('integer')).alias('next_date')).collect()
df.select(date_add('dt', -1).alias('prev_date')).collect()

#pyspark.sql.functions.date_format:
df = spark.createDataFrame([('2015-04-08',)], ['dt'])
df.select(date_format('dt', 'MM/dd/yyy').alias('date')).collect()

#pyspark.sql.functions.date_sub:
df = spark.createDataFrame([('2015-04-08', 2,)], ['dt', 'sub'])
df.select(date_sub(df.dt, 1).alias('prev_date')).collect()
df.select(date_sub(df.dt, df.sub.cast('integer')).alias('prev_date')).collect()
df.select(date_sub('dt', -1).alias('next_date')).collect()

#pyspark.sql.functions.date_trunc:
df = spark.createDataFrame([('1997-02-28 05:02:11',)], ['t'])
df.select(date_trunc('year', df.t).alias('year')).collect()
df.select(date_trunc('mon', df.t).alias('month')).collect()

#pyspark.sql.functions.datediff:
df = spark.createDataFrame([('2015-04-08','2015-05-10')], ['d1', 'd2'])
df.select(datediff(df.d2, df.d1).alias('diff')).collect()

#pyspark.sql.functions.dayofmonth:
df = spark.createDataFrame([('2015-04-08',)], ['dt'])
df.select(dayofmonth('dt').alias('day')).collect()

#pyspark.sql.functions.dayofweek:
df = spark.createDataFrame([('2015-04-08',)], ['dt'])
df.select(dayofweek('dt').alias('day')).collect()

#pyspark.sql.functions.dayofyear:
df = spark.createDataFrame([('2015-04-08',)], ['dt'])
df.select(dayofyear('dt').alias('day')).collect()

#pyspark.sql.functions.second:
import datetime
df = spark.createDataFrame([(datetime.datetime(2015, 4, 8, 13, 8, 15),)], ['ts'])
df.select(second('ts').alias('second')).collect()

#pyspark.sql.functions.weekofyear:
df = spark.createDataFrame([('2015-04-08',)], ['dt'])
df.select(weekofyear(df.dt).alias('week')).collect()

#pyspark.sql.functions.year:
df = spark.createDataFrame([('2015-04-08',)], ['dt'])
df.select(year('dt').alias('year')).collect()

#pyspark.sql.functions.quarter:
df = spark.createDataFrame([('2015-04-08',)], ['dt'])
df.select(quarter('dt').alias('quarter')).collect()

#pyspark.sql.functions.month:
df = spark.createDataFrame([('2015-04-08',)], ['dt'])
df.select(month('dt').alias('month')).collect()

#pyspark.sql.functions.last_day:
df = spark.createDataFrame([('1997-02-10',)], ['d'])
df.select(last_day(df.d).alias('date')).collect()

#pyspark.sql.functions.localtimestamp:
df = spark.range(1)
df.select(localtimestamp()).show(truncate=False)

#pyspark.sql.functions.minute:
import datetime
df = spark.createDataFrame([(datetime.datetime(2015, 4, 8, 13, 8, 15),)], ['ts'])
df.select(minute('ts').alias('minute')).collect()

#pyspark.sql.functions.months_between:
df = spark.createDataFrame([('1997-02-28 10:30:00', '1996-10-30')], ['date1', 'date2'])
df.select(months_between(df.date1, df.date2).alias('months')).collect()
df.select(months_between(df.date1, df.date2, False).alias('months')).collect()

#pyspark.sql.functions.next_day:
df = spark.createDataFrame([('2015-07-27',)], ['d'])
df.select(next_day(df.d, 'Sun').alias('date')).collect()

#pyspark.sql.functions.hour:
import datetime
df = spark.createDataFrame([(datetime.datetime(2015, 4, 8, 13, 8, 15),)], ['ts'])
df.select(hour('ts').alias('hour')).collect()

#pyspark.sql.functions.make_date:
df = spark.createDataFrame([(2020, 6, 26)], ['Y', 'M', 'D'])
df.select(make_date(df.Y, df.M, df.D).alias("datefield")).collect()

#pyspark.sql.functions.from_unixtime:
spark.conf.set("spark.sql.session.timeZone", "America/Los_Angeles")
time_df = spark.createDataFrame([(1428476400,)], ['unix_time'])
time_df.select(from_unixtime('unix_time').alias('ts')).collect()
spark.conf.unset("spark.sql.session.timeZone")

#pyspark.sql.functions.unix_timestamp:
spark.conf.set("spark.sql.session.timeZone", "America/Los_Angeles")
time_df = spark.createDataFrame([('2015-04-08',)], ['dt'])
time_df.select(unix_timestamp('dt', 'yyyy-MM-dd').alias('unix_time')).collect()
spark.conf.unset("spark.sql.session.timeZone")

#pyspark.sql.functions.to_timestamp:
df = spark.createDataFrame([('1997-02-28 10:30:00',)], ['t'])
df.select(to_timestamp(df.t).alias('dt')).collect()

df = spark.createDataFrame([('1997-02-28 10:30:00',)], ['t'])
df.select(to_timestamp(df.t, 'yyyy-MM-dd HH:mm:ss').alias('dt')).collect()

#pyspark.sql.functions.to_date:
df = spark.createDataFrame([('1997-02-28 10:30:00',)], ['t'])
df.select(to_date(df.t).alias('date')).collect()

df = spark.createDataFrame([('1997-02-28 10:30:00',)], ['t'])
df.select(to_date(df.t, 'yyyy-MM-dd HH:mm:ss').alias('date')).collect()

#pyspark.sql.functions.trunc:
df = spark.createDataFrame([('1997-02-28',)], ['d'])
df.select(trunc(df.d, 'year').alias('year')).collect()
df.select(trunc(df.d, 'mon').alias('month')).collect()

#pyspark.sql.functions.from_utc_timestamp:
df = spark.createDataFrame([('1997-02-28 10:30:00', 'JST')], ['ts', 'tz'])
df.select(from_utc_timestamp(df.ts, "PST").alias('local_time')).collect()
df.select(from_utc_timestamp(df.ts, df.tz).alias('local_time')).collect()

#pyspark.sql.functions.to_utc_timestamp:
df = spark.createDataFrame([('1997-02-28 10:30:00', 'JST')], ['ts', 'tz'])
df.select(to_utc_timestamp(df.ts, "PST").alias('utc_time')).collect()
df.select(to_utc_timestamp(df.ts, df.tz).alias('utc_time')).collect()

#pyspark.sql.functions.window:
import datetime
df = spark.createDataFrame(
[(datetime.datetime(2016, 3, 11, 9, 0, 7), 1)],
).toDF("date", "val")
w = df.groupBy(window("date", "5 seconds")).agg(sum("val").alias("sum"))
w.select(w.window.start.cast("string").alias("start"),
w.window.end.cast("string").alias("end"), "sum").collect()

#pyspark.sql.functions.session_window:
df = spark.createDataFrame([("2016-03-11 09:00:07", 1)]).toDF("date", "val")
w = df.groupBy(session_window("date", "5 seconds")).agg(sum("val").alias("sum"))
w.select(w.session_window.start.cast("string").alias("start"),
w.session_window.end.cast("string").alias("end"), "sum").collect()
w = df.groupBy(session_window("date", lit("5 seconds"))).agg(sum("val").alias("sum"))
w.select(w.session_window.start.cast("string").alias("start"),
w.session_window.end.cast("string").alias("end"), "sum").collect()

#pyspark.sql.functions.timestamp_seconds:
from pyspark.sql.functions import timestamp_seconds
spark.conf.set("spark.sql.session.timeZone", "UTC")
time_df = spark.createDataFrame([(1230219000,)], ['unix_time'])
time_df.select(timestamp_seconds(time_df.unix_time).alias('ts')).show()
time_df.select(timestamp_seconds('unix_time').alias('ts')).printSchema()
spark.conf.unset("spark.sql.session.timeZone")

#pyspark.sql.functions.window_time:
import datetime
df = spark.createDataFrame(
[(datetime.datetime(2016, 3, 11, 9, 0, 7), 1)],
).toDF("date", "val")

w = df.groupBy(window("date", "5 seconds")).agg(sum("val").alias("sum"))

w.select(
w.window.end.cast("string").alias("end"),
window_time(w.window).cast("string").alias("window_time"),
"sum"
).collect()

#pyspark.sql.functions.array:
df = spark.createDataFrame([("Alice", 2), ("Bob", 5)], ("name", "age"))
df.select(array('age', 'age').alias("arr")).collect()
df.select(array([df.age, df.age]).alias("arr")).collect()
df.select(array('age', 'age').alias("col")).printSchema()

#pyspark.sql.functions.array_contains:
df = spark.createDataFrame([(["a", "b", "c"],), ([],)], ['data'])
df.select(array_contains(df.data, "a")).collect()
df.select(array_contains(df.data, lit("a"))).collect()

#pyspark.sql.functions.arrays_overlap:
df = spark.createDataFrame([(["a", "b"], ["b", "c"]), (["a"], ["b", "c"])], ['x', 'y'])
df.select(arrays_overlap(df.x, df.y).alias("overlap")).collect()

#pyspark.sql.functions.array_join:
df = spark.createDataFrame([(["a", "b", "c"],), (["a", None],)], ['data'])
df.select(array_join(df.data, ",").alias("joined")).collect()
df.select(array_join(df.data, ",", "NULL").alias("joined")).collect()

#pyspark.sql.functions.create_map:
df = spark.createDataFrame([("Alice", 2), ("Bob", 5)], ("name", "age"))
df.select(create_map('name', 'age').alias("map")).collect()
df.select(create_map([df.name, df.age]).alias("map")).collect()

#pyspark.sql.functions.slice:
df = spark.createDataFrame([([1, 2, 3],), ([4, 5],)], ['x'])
df.select(slice(df.x, 2, 2).alias("sliced")).collect()

#pyspark.sql.functions.concat:
df = spark.createDataFrame([('abcd','123')], ['s', 'd'])
df = df.select(concat(df.s, df.d).alias('s'))
df.collect()
df

df = spark.createDataFrame([([1, 2], [3, 4], [5]), ([1, 2], None, [3])], ['a', 'b', 'c'])
df = df.select(concat(df.a, df.b, df.c).alias("arr"))
df.collect()
df

#pyspark.sql.functions.array_position:
df = spark.createDataFrame([(["c", "b", "a"],), ([],)], ['data'])
df.select(array_position(df.data, "a")).collect()

#pyspark.sql.functions.element_at:
df = spark.createDataFrame([(["a", "b", "c"],)], ['data'])
df.select(element_at(df.data, 1)).collect()
df.select(element_at(df.data, -1)).collect()

df = spark.createDataFrame([({"a": 1.0, "b": 2.0},)], ['data'])
df.select(element_at(df.data, lit("a"))).collect()

#pyspark.sql.functions.array_append:
from pyspark.sql import Row
df = spark.createDataFrame([Row(c1=["b", "a", "c"], c2="c")])
df.select(array_append(df.c1, df.c2)).collect()
df.select(array_append(df.c1, 'x')).collect()

#pyspark.sql.functions.array_sort:
df = spark.createDataFrame([([2, 1, None, 3],),([1],),([],)], ['data'])
df.select(array_sort(df.data).alias('r')).collect()
df = spark.createDataFrame([(["foo", "foobar", None, "bar"],),(["foo"],),([],)], ['data'])
df.select(array_sort(
"data",
lambda x, y: when(x.isNull() | y.isNull(), lit(0)).otherwise(length(y) - length(x))
).alias("r")).collect()

#pyspark.sql.functions.array_insert:
df = spark.createDataFrame(
[(['a', 'b', 'c'], 2, 'd'), (['c', 'b', 'a'], -2, 'd')],
['data', 'pos', 'val']
)
df.select(array_insert(df.data, df.pos.cast('integer'), df.val).alias('data')).collect()
df.select(array_insert(df.data, 5, 'hello').alias('data')).collect()

#pyspark.sql.functions.array_remove:
df = spark.createDataFrame([([1, 2, 3, 1, 1],), ([],)], ['data'])
df.select(array_remove(df.data, 1)).collect()

#pyspark.sql.functions.array_distinct:
df = spark.createDataFrame([([1, 2, 3, 2],), ([4, 5, 5, 4],)], ['data'])
df.select(array_distinct(df.data)).collect()

#pyspark.sql.functions.array_intersect:
from pyspark.sql import Row
df = spark.createDataFrame([Row(c1=["b", "a", "c"], c2=["c", "d", "a", "f"])])
df.select(array_intersect(df.c1, df.c2)).collect()

#pyspark.sql.functions.array_union:
from pyspark.sql import Row
df = spark.createDataFrame([Row(c1=["b", "a", "c"], c2=["c", "d", "a", "f"])])
df.select(array_union(df.c1, df.c2)).collect()

#pyspark.sql.functions.array_except:
from pyspark.sql import Row
df = spark.createDataFrame([Row(c1=["b", "a", "c"], c2=["c", "d", "a", "f"])])
df.select(array_except(df.c1, df.c2)).collect()

#pyspark.sql.functions.array_compact:
df = spark.createDataFrame([([1, None, 2, 3],), ([4, 5, None, 4],)], ['data'])
df.select(array_compact(df.data)).collect()

#pyspark.sql.functions.transform:
df = spark.createDataFrame([(1, [1, 2, 3, 4])], ("key", "values"))
df.select(transform("values", lambda x: x * 2).alias("doubled")).show()

def alternate(x, i):
return when(i % 2 == 0, x).otherwise(-x)
df.select(transform("values", alternate).alias("alternated")).show()

#pyspark.sql.functions.exists:
df = spark.createDataFrame([(1, [1, 2, 3, 4]), (2, [3, -1, 0])],("key", "values"))
df.select(exists("values", lambda x: x < 0).alias("any_negative")).show()

#pyspark.sql.functions.forall:
df = spark.createDataFrame(
[(1, ["bar"]), (2, ["foo", "bar"]), (3, ["foobar", "foo"])],
("key", "values")
)
df.select(forall("values", lambda x: x.rlike("foo")).alias("all_foo")).show()

#pyspark.sql.functions.filter:
df = spark.createDataFrame(
[(1, ["2018-09-20",  "2019-02-03", "2019-07-01", "2020-06-01"])],
("key", "values")
)
def after_second_quarter(x):
return month(to_date(x)) > 6
df.select(
filter("values", after_second_quarter).alias("after_second_quarter")
).show(truncate=False)

#pyspark.sql.functions.aggregate:
df = spark.createDataFrame([(1, [20.0, 4.0, 2.0, 6.0, 10.0])], ("id", "values"))
df.select(aggregate("values", lit(0.0), lambda acc, x: acc + x).alias("sum")).show()

def merge(acc, x):
count = acc.count + 1
sum = acc.sum + x
return struct(count.alias("count"), sum.alias("sum"))
df.select(
aggregate(
"values",
struct(lit(0).alias("count"), lit(0.0).alias("sum")),
merge,
lambda acc: acc.sum / acc.count,
).alias("mean")
).show()

#pyspark.sql.functions.zip_with:
df = spark.createDataFrame([(1, [1, 3, 5, 8], [0, 2, 4, 6])], ("id", "xs", "ys"))
df.select(zip_with("xs", "ys", lambda x, y: x ** y).alias("powers")).show(truncate=False)

df = spark.createDataFrame([(1, ["foo", "bar"], [1, 2, 3])], ("id", "xs", "ys"))
df.select(zip_with("xs", "ys", lambda x, y: concat_ws("_", x, y)).alias("xs_ys")).show()

#pyspark.sql.functions.transform_keys:
df = spark.createDataFrame([(1, {"foo": -2.0, "bar": 2.0})], ("id", "data"))
row = df.select(transform_keys(
"data", lambda k, _: upper(k)).alias("data_upper")
).head()
sorted(row["data_upper"].items())

#pyspark.sql.functions.transform_values:
df = spark.createDataFrame([(1, {"IT": 10.0, "SALES": 2.0, "OPS": 24.0})], ("id", "data"))
row = df.select(transform_values(
"data", lambda k, v: when(k.isin("IT", "OPS"), v + 10.0).otherwise(v)
).alias("new_data")).head()
sorted(row["new_data"].items())

#pyspark.sql.functions.map_filter:
df = spark.createDataFrame([(1, {"foo": 42.0, "bar": 1.0, "baz": 32.0})], ("id", "data"))
row = df.select(map_filter(
"data", lambda _, v: v > 30.0).alias("data_filtered")
).head()
sorted(row["data_filtered"].items())

#pyspark.sql.functions.map_from_arrays:
df = spark.createDataFrame([([2, 5], ['a', 'b'])], ['k', 'v'])
df = df.select(map_from_arrays(df.k, df.v).alias("col"))
df.show()
df.printSchema()

#pyspark.sql.functions.map_zip_with:
df = spark.createDataFrame([
(1, {"IT": 24.0, "SALES": 12.00}, {"IT": 2.0, "SALES": 1.4})],
("id", "base", "ratio")
)
row = df.select(map_zip_with(
"base", "ratio", lambda k, v1, v2: round(v1 * v2, 2)).alias("updated_data")
).head()
sorted(row["updated_data"].items())

#pyspark.sql.functions.explode:
from pyspark.sql import Row
eDF = spark.createDataFrame([Row(a=1, intlist=[1,2,3], mapfield={"a": "b"})])
eDF.select(explode(eDF.intlist).alias("anInt")).collect()

eDF.select(explode(eDF.mapfield).alias("key", "value")).show()

#pyspark.sql.functions.explode_outer:
df = spark.createDataFrame(
[(1, ["foo", "bar"], {"x": 1.0}), (2, [], {}), (3, None, None)],
("id", "an_array", "a_map")
)
df.select("id", "an_array", explode_outer("a_map")).show()

df.select("id", "a_map", explode_outer("an_array")).show()

#pyspark.sql.functions.posexplode:
from pyspark.sql import Row
eDF = spark.createDataFrame([Row(a=1, intlist=[1,2,3], mapfield={"a": "b"})])
eDF.select(posexplode(eDF.intlist)).collect()

eDF.select(posexplode(eDF.mapfield)).show()

#pyspark.sql.functions.posexplode_outer:
df = spark.createDataFrame(
[(1, ["foo", "bar"], {"x": 1.0}), (2, [], {}), (3, None, None)],
("id", "an_array", "a_map")
)
df.select("id", "an_array", posexplode_outer("a_map")).show()
df.select("id", "a_map", posexplode_outer("an_array")).show()

#pyspark.sql.functions.inline:
from pyspark.sql import Row
df = spark.createDataFrame([Row(structlist=[Row(a=1, b=2), Row(a=3, b=4)])])
df.select(inline(df.structlist)).show()

#pyspark.sql.functions.inline_outer:
from pyspark.sql import Row
df = spark.createDataFrame([
Row(id=1, structlist=[Row(a=1, b=2), Row(a=3, b=4)]),
Row(id=2, structlist=[])
])
df.select('id', inline_outer(df.structlist)).show()

#pyspark.sql.functions.get:
df = spark.createDataFrame([(["a", "b", "c"], 1)], ['data', 'index'])
df.select(get(df.data, 1)).show()

df.select(get(df.data, -1)).show()

df.select(get(df.data, 3)).show()

df.select(get(df.data, "index")).show()

df.select(get(df.data, col("index") - 1)).show()

#pyspark.sql.functions.get_json_object:
data = [("1", '''{"f1": "value1", "f2": "value2"}'''), ("2", '''{"f1": "value12"}''')]
df = spark.createDataFrame(data, ("key", "jstring"))
df.select(df.key, get_json_object(df.jstring, '$.f1').alias("c0"), \
get_json_object(df.jstring, '$.f2').alias("c1") ).collect()

#pyspark.sql.functions.json_tuple:
data = [("1", '''{"f1": "value1", "f2": "value2"}'''), ("2", '''{"f1": "value12"}''')]
df = spark.createDataFrame(data, ("key", "jstring"))
df.select(df.key, json_tuple(df.jstring, 'f1', 'f2')).collect()

#pyspark.sql.functions.from_json:
from pyspark.sql.types import *
data = [(1, '''{"a": 1}''')]
schema = StructType([StructField("a", IntegerType())])
df = spark.createDataFrame(data, ("key", "value"))
df.select(from_json(df.value, schema).alias("json")).collect()
df.select(from_json(df.value, "a INT").alias("json")).collect()
df.select(from_json(df.value, "MAP<STRING,INT>").alias("json")).collect()
data = [(1, '''[{"a": 1}]''')]
schema = ArrayType(StructType([StructField("a", IntegerType())]))
df = spark.createDataFrame(data, ("key", "value"))
df.select(from_json(df.value, schema).alias("json")).collect()
schema = schema_of_json(lit('''{"a": 0}'''))
df.select(from_json(df.value, schema).alias("json")).collect()
data = [(1, '''[1, 2, 3]''')]
schema = ArrayType(IntegerType())
df = spark.createDataFrame(data, ("key", "value"))
df.select(from_json(df.value, schema).alias("json")).collect()

#pyspark.sql.functions.schema_of_json:
df = spark.range(1)
df.select(schema_of_json(lit('{"a": 0}')).alias("json")).collect()
schema = schema_of_json('{a: 1}', {'allowUnquotedFieldNames':'true'})
df.select(schema.alias("json")).collect()

#pyspark.sql.functions.to_json:
from pyspark.sql import Row
from pyspark.sql.types import *
data = [(1, Row(age=2, name='Alice'))]
df = spark.createDataFrame(data, ("key", "value"))
df.select(to_json(df.value).alias("json")).collect()
data = [(1, [Row(age=2, name='Alice'), Row(age=3, name='Bob')])]
df = spark.createDataFrame(data, ("key", "value"))
df.select(to_json(df.value).alias("json")).collect()
data = [(1, {"name": "Alice"})]
df = spark.createDataFrame(data, ("key", "value"))
df.select(to_json(df.value).alias("json")).collect()
data = [(1, [{"name": "Alice"}, {"name": "Bob"}])]
df = spark.createDataFrame(data, ("key", "value"))
df.select(to_json(df.value).alias("json")).collect()
data = [(1, ["Alice", "Bob"])]
df = spark.createDataFrame(data, ("key", "value"))
df.select(to_json(df.value).alias("json")).collect()

#pyspark.sql.functions.size:
df = spark.createDataFrame([([1, 2, 3],),([1],),([],)], ['data'])
df.select(size(df.data)).collect()

#pyspark.sql.functions.struct:
df = spark.createDataFrame([("Alice", 2), ("Bob", 5)], ("name", "age"))
df.select(struct('age', 'name').alias("struct")).collect()
df.select(struct([df.age, df.name]).alias("struct")).collect()

#pyspark.sql.functions.sort_array:
df = spark.createDataFrame([([2, 1, None, 3],),([1],),([],)], ['data'])
df.select(sort_array(df.data).alias('r')).collect()
df.select(sort_array(df.data, asc=False).alias('r')).collect()

#pyspark.sql.functions.array_max:
df = spark.createDataFrame([([2, 1, 3],), ([None, 10, -1],)], ['data'])
df.select(array_max(df.data).alias('max')).collect()

#pyspark.sql.functions.array_min:
df = spark.createDataFrame([([2, 1, 3],), ([None, 10, -1],)], ['data'])
df.select(array_min(df.data).alias('min')).collect()

#pyspark.sql.functions.shuffle:
df = spark.createDataFrame([([1, 20, 3, 5],), ([1, 20, None, 3],)], ['data'])
df.select(shuffle(df.data).alias('s')).collect()

#pyspark.sql.functions.reverse:
df = spark.createDataFrame([('Spark SQL',)], ['data'])
df.select(reverse(df.data).alias('s')).collect()
df = spark.createDataFrame([([2, 1, 3],) ,([1],) ,([],)], ['data'])
df.select(reverse(df.data).alias('r')).collect()

#pyspark.sql.functions.flatten:
df = spark.createDataFrame([([[1, 2, 3], [4, 5], [6]],), ([None, [4, 5]],)], ['data'])
df.show(truncate=False)
df.select(flatten(df.data).alias('r')).show()

#pyspark.sql.functions.sequence:
df1 = spark.createDataFrame([(-2, 2)], ('C1', 'C2'))
df1.select(sequence('C1', 'C2').alias('r')).collect()
df2 = spark.createDataFrame([(4, -4, -2)], ('C1', 'C2', 'C3'))
df2.select(sequence('C1', 'C2', 'C3').alias('r')).collect()

#pyspark.sql.functions.array_repeat:
df = spark.createDataFrame([('ab',)], ['data'])
df.select(array_repeat(df.data, 3).alias('r')).collect()

#pyspark.sql.functions.map_contains_key:
from pyspark.sql.functions import map_contains_key
df = spark.sql("SELECT map(1, 'a', 2, 'b') as data")
df.select(map_contains_key("data", 1)).show()
df.select(map_contains_key("data", -1)).show()

#pyspark.sql.functions.map_keys:
from pyspark.sql.functions import map_keys
df = spark.sql("SELECT map(1, 'a', 2, 'b') as data")
df.select(map_keys("data").alias("keys")).show()

#pyspark.sql.functions.map_values:
from pyspark.sql.functions import map_values
df = spark.sql("SELECT map(1, 'a', 2, 'b') as data")
df.select(map_values("data").alias("values")).show()

#pyspark.sql.functions.map_entries:
from pyspark.sql.functions import map_entries
df = spark.sql("SELECT map(1, 'a', 2, 'b') as data")
df = df.select(map_entries("data").alias("entries"))
df.show()
df.printSchema()

#pyspark.sql.functions.map_from_entries:
from pyspark.sql.functions import map_from_entries
df = spark.sql("SELECT array(struct(1, 'a'), struct(2, 'b')) as data")
df.select(map_from_entries("data").alias("map")).show()

#pyspark.sql.functions.arrays_zip:
from pyspark.sql.functions import arrays_zip
df = spark.createDataFrame([(([1, 2, 3], [2, 4, 6], [3, 6]))], ['vals1', 'vals2', 'vals3'])
df = df.select(arrays_zip(df.vals1, df.vals2, df.vals3).alias('zipped'))
df.show(truncate=False)
df.printSchema()

#pyspark.sql.functions.map_concat:
from pyspark.sql.functions import map_concat
df = spark.sql("SELECT map(1, 'a', 2, 'b') as map1, map(3, 'c') as map2")
df.select(map_concat("map1", "map2").alias("map3")).show(truncate=False)

#pyspark.sql.functions.from_csv:
data = [("1,2,3",)]
df = spark.createDataFrame(data, ("value",))
df.select(from_csv(df.value, "a INT, b INT, c INT").alias("csv")).collect()
value = data[0][0]
df.select(from_csv(df.value, schema_of_csv(value)).alias("csv")).collect()
data = [("   abc",)]
df = spark.createDataFrame(data, ("value",))
options = {'ignoreLeadingWhiteSpace': True}
df.select(from_csv(df.value, "s string", options).alias("csv")).collect()

#pyspark.sql.functions.schema_of_csv:
df = spark.range(1)
df.select(schema_of_csv(lit('1|a'), {'sep':'|'}).alias("csv")).collect()
df.select(schema_of_csv('1|a', {'sep':'|'}).alias("csv")).collect()

#pyspark.sql.functions.to_csv:
from pyspark.sql import Row
data = [(1, Row(age=2, name='Alice'))]
df = spark.createDataFrame(data, ("key", "value"))
df.select(to_csv(df.value).alias("csv")).collect()

#pyspark.sql.functions.years:
df.writeTo("catalog.db.table").partitionedBy(
years("ts")
).createOrReplace()

#pyspark.sql.functions.months:
df.writeTo("catalog.db.table").partitionedBy(
months("ts")
).createOrReplace()

#pyspark.sql.functions.days:
df.writeTo("catalog.db.table").partitionedBy(
days("ts")
).createOrReplace()

#pyspark.sql.functions.hours:
df.writeTo("catalog.db.table").partitionedBy(
hours("ts")
).createOrReplace()

#pyspark.sql.functions.bucket:
df.writeTo("catalog.db.table").partitionedBy(
bucket(42, "ts")
).createOrReplace()

#pyspark.sql.functions.approxCountDistinct:
#pyspark.sql.functions.approx_count_distinct:
df = spark.createDataFrame([1,2,2,3], "INT")
df.agg(approx_count_distinct("value").alias('distinct_values')).show()

#pyspark.sql.functions.avg:
df = spark.range(10)
df.select(avg(col("id"))).show()

#pyspark.sql.functions.collect_list:
df2 = spark.createDataFrame([(2,), (5,), (5,)], ('age',))
df2.agg(collect_list('age')).collect()

#pyspark.sql.functions.collect_set:
df2 = spark.createDataFrame([(2,), (5,), (5,)], ('age',))
df2.agg(array_sort(collect_set('age')).alias('c')).collect()

#pyspark.sql.functions.corr:
a = range(20)
b = [2 * x for x in range(20)]
df = spark.createDataFrame(zip(a, b), ["a", "b"])
df.agg(corr("a", "b").alias('c')).collect()

#pyspark.sql.functions.count:
df = spark.createDataFrame([(None,), ("a",), ("b",), ("c",)], schema=["alphabets"])
df.select(count(expr("*")), count(df.alphabets)).show()

#pyspark.sql.functions.count_distinct:
from pyspark.sql import types
df1 = spark.createDataFrame([1, 1, 3], types.IntegerType())
df2 = spark.createDataFrame([1, 2], types.IntegerType())
df1.join(df2).show()
df1.join(df2).select(count_distinct(df1.value, df2.value)).show()

#pyspark.sql.functions.countDistinct:
#pyspark.sql.functions.covar_pop:
a = [1] * 10
b = [1] * 10
df = spark.createDataFrame(zip(a, b), ["a", "b"])
df.agg(covar_pop("a", "b").alias('c')).collect()

#pyspark.sql.functions.covar_samp:
a = [1] * 10
b = [1] * 10
df = spark.createDataFrame(zip(a, b), ["a", "b"])
df.agg(covar_samp("a", "b").alias('c')).collect()

#pyspark.sql.functions.first:
df = spark.createDataFrame([("Alice", 2), ("Bob", 5), ("Alice", None)], ("name", "age"))
df = df.orderBy(df.age)
df.groupby("name").agg(first("age")).orderBy("name").show()

df.groupby("name").agg(first("age", ignorenulls=True)).orderBy("name").show()

#pyspark.sql.functions.grouping:
df = spark.createDataFrame([("Alice", 2), ("Bob", 5)], ("name", "age"))
df.cube("name").agg(grouping("name"), sum("age")).orderBy("name").show()

#pyspark.sql.functions.grouping_id:
df = spark.createDataFrame([(1, "a", "a"),
(3, "a", "a"),
(4, "b", "c")], ["c1", "c2", "c3"])
df.cube("c2", "c3").agg(grouping_id(), sum("c1")).orderBy("c2", "c3").show()

#pyspark.sql.functions.kurtosis:
df = spark.createDataFrame([[1],[1],[2]], ["c"])
df.select(kurtosis(df.c)).show()

#pyspark.sql.functions.last:
df = spark.createDataFrame([("Alice", 2), ("Bob", 5), ("Alice", None)], ("name", "age"))
df = df.orderBy(df.age.desc())
df.groupby("name").agg(last("age")).orderBy("name").show()

df.groupby("name").agg(last("age", ignorenulls=True)).orderBy("name").show()

#pyspark.sql.functions.max:
df = spark.range(10)
df.select(max(col("id"))).show()

#pyspark.sql.functions.max_by:
df = spark.createDataFrame([
("Java", 2012, 20000), ("dotNET", 2012, 5000),
("dotNET", 2013, 48000), ("Java", 2013, 30000)],
schema=("course", "year", "earnings"))
df.groupby("course").agg(max_by("year", "earnings")).show()

#pyspark.sql.functions.mean:
df = spark.range(10)
df.select(mean(df.id)).show()

#pyspark.sql.functions.median:
df = spark.createDataFrame([
("Java", 2012, 20000), ("dotNET", 2012, 5000),
("Java", 2012, 22000), ("dotNET", 2012, 10000),
("dotNET", 2013, 48000), ("Java", 2013, 30000)],
schema=("course", "year", "earnings"))
df.groupby("course").agg(median("earnings")).show()

#pyspark.sql.functions.min:
df = spark.range(10)
df.select(min(df.id)).show()

#pyspark.sql.functions.min_by:
df = spark.createDataFrame([
("Java", 2012, 20000), ("dotNET", 2012, 5000),
("dotNET", 2013, 48000), ("Java", 2013, 30000)],
schema=("course", "year", "earnings"))
df.groupby("course").agg(min_by("year", "earnings")).show()

#pyspark.sql.functions.mode:
df = spark.createDataFrame([
("Java", 2012, 20000), ("dotNET", 2012, 5000),
("Java", 2012, 20000), ("dotNET", 2012, 5000),
("dotNET", 2013, 48000), ("Java", 2013, 30000)],
schema=("course", "year", "earnings"))
df.groupby("course").agg(mode("year")).show()

#pyspark.sql.functions.percentile_approx:
key = (col("id") % 3).alias("key")
value = (randn(42) + key * 10).alias("value")
df = spark.range(0, 1000, 1, 1).select(key, value)
df.select(
percentile_approx("value", [0.25, 0.5, 0.75], 1000000).alias("quantiles")
).printSchema()

df.groupBy("key").agg(
percentile_approx("value", 0.5, lit(1000000)).alias("median")
).printSchema()

#pyspark.sql.functions.product:
df = spark.range(1, 10).toDF('x').withColumn('mod3', col('x') % 3)
prods = df.groupBy('mod3').agg(product('x').alias('product'))
prods.orderBy('mod3').show()

#pyspark.sql.functions.skewness:
df = spark.createDataFrame([[1],[1],[2]], ["c"])
df.select(skewness(df.c)).first()

#pyspark.sql.functions.stddev:
df = spark.range(6)
df.select(stddev(df.id)).first()

#pyspark.sql.functions.stddev_pop:
df = spark.range(6)
df.select(stddev_pop(df.id)).first()

#pyspark.sql.functions.stddev_samp:
df = spark.range(6)
df.select(stddev_samp(df.id)).first()

#pyspark.sql.functions.sum:
df = spark.range(10)
df.select(sum(df["id"])).show()

#pyspark.sql.functions.sum_distinct:
df = spark.createDataFrame([(None,), (1,), (1,), (2,)], schema=["numbers"])
df.select(sum_distinct(col("numbers"))).show()

#pyspark.sql.functions.sumDistinct:
#pyspark.sql.functions.var_pop:
df = spark.range(6)
df.select(var_pop(df.id)).first()

#pyspark.sql.functions.var_samp:
df = spark.range(6)
df.select(var_samp(df.id)).show()

#pyspark.sql.functions.variance:
df = spark.range(6)
df.select(variance(df.id)).show()

#pyspark.sql.functions.cume_dist:
from pyspark.sql import Window, types
df = spark.createDataFrame([1, 2, 3, 3, 4], types.IntegerType())
w = Window.orderBy("value")
df.withColumn("cd", cume_dist().over(w)).show()

#pyspark.sql.functions.dense_rank:
from pyspark.sql import Window, types
df = spark.createDataFrame([1, 1, 2, 3, 3, 4], types.IntegerType())
w = Window.orderBy("value")
df.withColumn("drank", dense_rank().over(w)).show()

#pyspark.sql.functions.lag:
from pyspark.sql import Window
df = spark.createDataFrame([("a", 1),
("a", 2),
("a", 3),
("b", 8),
("b", 2)], ["c1", "c2"])
df.show()
w = Window.partitionBy("c1").orderBy("c2")
df.withColumn("previos_value", lag("c2").over(w)).show()
df.withColumn("previos_value", lag("c2", 1, 0).over(w)).show()
df.withColumn("previos_value", lag("c2", 2, -1).over(w)).show()

#pyspark.sql.functions.lead:
from pyspark.sql import Window
df = spark.createDataFrame([("a", 1),
("a", 2),
("a", 3),
("b", 8),
("b", 2)], ["c1", "c2"])
df.show()
w = Window.partitionBy("c1").orderBy("c2")
df.withColumn("next_value", lead("c2").over(w)).show()
df.withColumn("next_value", lead("c2", 1, 0).over(w)).show()
df.withColumn("next_value", lead("c2", 2, -1).over(w)).show()

#pyspark.sql.functions.nth_value:
from pyspark.sql import Window
df = spark.createDataFrame([("a", 1),
("a", 2),
("a", 3),
("b", 8),
("b", 2)], ["c1", "c2"])
df.show()
w = Window.partitionBy("c1").orderBy("c2")
df.withColumn("nth_value", nth_value("c2", 1).over(w)).show()
df.withColumn("nth_value", nth_value("c2", 2).over(w)).show()

#pyspark.sql.functions.ntile:
from pyspark.sql import Window
df = spark.createDataFrame([("a", 1),
("a", 2),
("a", 3),
("b", 8),
("b", 2)], ["c1", "c2"])
df.show()
w = Window.partitionBy("c1").orderBy("c2")
df.withColumn("ntile", ntile(2).over(w)).show()

#pyspark.sql.functions.percent_rank:
from pyspark.sql import Window, types
df = spark.createDataFrame([1, 1, 2, 3, 3, 4], types.IntegerType())
w = Window.orderBy("value")
df.withColumn("pr", percent_rank().over(w)).show()

#pyspark.sql.functions.rank:
from pyspark.sql import Window, types
df = spark.createDataFrame([1, 1, 2, 3, 3, 4], types.IntegerType())
w = Window.orderBy("value")
df.withColumn("drank", rank().over(w)).show()

#pyspark.sql.functions.row_number:
from pyspark.sql import Window
df = spark.range(3)
w = Window.orderBy(df.id.desc())
df.withColumn("desc_order", row_number().over(w)).show()

#pyspark.sql.functions.asc:
df = spark.range(5)
df = df.sort(desc("id"))
df.show()

df.orderBy(asc("id")).show()

#pyspark.sql.functions.asc_nulls_first:
df1 = spark.createDataFrame([(1, "Bob"),
(0, None),
(2, "Alice")], ["age", "name"])
df1.sort(asc_nulls_first(df1.name)).show()

#pyspark.sql.functions.asc_nulls_last:
df1 = spark.createDataFrame([(0, None),
(1, "Bob"),
(2, "Alice")], ["age", "name"])
df1.sort(asc_nulls_last(df1.name)).show()

#pyspark.sql.functions.desc:
spark.range(5).orderBy(desc("id")).show()

#pyspark.sql.functions.desc_nulls_first:
df1 = spark.createDataFrame([(0, None),
(1, "Bob"),
(2, "Alice")], ["age", "name"])
df1.sort(desc_nulls_first(df1.name)).show()

#pyspark.sql.functions.desc_nulls_last:
df1 = spark.createDataFrame([(0, None),
(1, "Bob"),
(2, "Alice")], ["age", "name"])
df1.sort(desc_nulls_last(df1.name)).show()

#pyspark.sql.functions.ascii:
df = spark.createDataFrame(["Spark", "PySpark", "Pandas API"], "STRING")
df.select(ascii("value")).show()

#pyspark.sql.functions.base64:
df = spark.createDataFrame(["Spark", "PySpark", "Pandas API"], "STRING")
df.select(base64("value")).show()

#pyspark.sql.functions.bit_length:
from pyspark.sql.functions import bit_length
spark.createDataFrame([('cat',), ( 'ð',)], ['cat']) \
select(bit_length('cat')).collect()

#pyspark.sql.functions.concat_ws:
df = spark.createDataFrame([('abcd','123')], ['s', 'd'])
df.select(concat_ws('-', df.s, df.d).alias('s')).collect()

#pyspark.sql.functions.decode:
df = spark.createDataFrame([('abcd',)], ['a'])
df.select(decode("a", "UTF-8")).show()

#pyspark.sql.functions.encode:
df = spark.createDataFrame([('abcd',)], ['c'])
df.select(encode("c", "UTF-8")).show()

#pyspark.sql.functions.format_number:
spark.createDataFrame([(5,)], ['a']).select(format_number('a', 4).alias('v')).collect()

#pyspark.sql.functions.format_string:
df = spark.createDataFrame([(5, "hello")], ['a', 'b'])
df.select(format_string('%d %s', df.a, df.b).alias('v')).collect()

#pyspark.sql.functions.initcap:
spark.createDataFrame([('ab cd',)], ['a']).select(initcap("a").alias('v')).collect()

#pyspark.sql.functions.instr:
df = spark.createDataFrame([('abcd',)], ['s',])
df.select(instr(df.s, 'b').alias('s')).collect()

#pyspark.sql.functions.length:
spark.createDataFrame([('ABC ',)], ['a']).select(length('a').alias('length')).collect()

#pyspark.sql.functions.lower:
df = spark.createDataFrame(["Spark", "PySpark", "Pandas API"], "STRING")
df.select(lower("value")).show()

#pyspark.sql.functions.levenshtein:
df0 = spark.createDataFrame([('kitten', 'sitting',)], ['l', 'r'])
df0.select(levenshtein('l', 'r').alias('d')).collect()

#pyspark.sql.functions.locate:
df = spark.createDataFrame([('abcd',)], ['s',])
df.select(locate('b', df.s, 1).alias('s')).collect()

#pyspark.sql.functions.lpad:
df = spark.createDataFrame([('abcd',)], ['s',])
df.select(lpad(df.s, 6, '#').alias('s')).collect()

#pyspark.sql.functions.ltrim:
df = spark.createDataFrame(["   Spark", "Spark  ", " Spark"], "STRING")
df.select(ltrim("value").alias("r")).withColumn("length", length("r")).show()

#pyspark.sql.functions.octet_length:
from pyspark.sql.functions import octet_length
spark.createDataFrame([('cat',), ( 'ð',)], ['cat']) \
select(octet_length('cat')).collect()

#pyspark.sql.functions.regexp_extract:
df = spark.createDataFrame([('100-200',)], ['str'])
df.select(regexp_extract('str', r'(\d+)-(\d+)', 1).alias('d')).collect()
df = spark.createDataFrame([('foo',)], ['str'])
df.select(regexp_extract('str', r'(\d+)', 1).alias('d')).collect()
df = spark.createDataFrame([('aaaac',)], ['str'])
df.select(regexp_extract('str', '(a+)(b)?(c)', 2).alias('d')).collect()

#pyspark.sql.functions.regexp_replace:
df = spark.createDataFrame([("100-200", r"(\d+)", "--")], ["str", "pattern", "replacement"])
df.select(regexp_replace('str', r'(\d+)', '--').alias('d')).collect()
df.select(regexp_replace("str", col("pattern"), col("replacement")).alias('d')).collect()

#pyspark.sql.functions.unbase64:
df = spark.createDataFrame(["U3Bhcms=",
"UHlTcGFyaw==",
"UGFuZGFzIEFQSQ=="], "STRING")
df.select(unbase64("value")).show()

#pyspark.sql.functions.rpad:
df = spark.createDataFrame([('abcd',)], ['s',])
df.select(rpad(df.s, 6, '#').alias('s')).collect()

#pyspark.sql.functions.repeat:
df = spark.createDataFrame([('ab',)], ['s',])
df.select(repeat(df.s, 3).alias('s')).collect()

#pyspark.sql.functions.rtrim:
df = spark.createDataFrame(["   Spark", "Spark  ", " Spark"], "STRING")
df.select(rtrim("value").alias("r")).withColumn("length", length("r")).show()

#pyspark.sql.functions.soundex:
df = spark.createDataFrame([("Peters",),("Uhrbach",)], ['name'])
df.select(soundex(df.name).alias("soundex")).collect()

#pyspark.sql.functions.split:
df = spark.createDataFrame([('oneAtwoBthreeC',)], ['s',])
df.select(split(df.s, '[ABC]', 2).alias('s')).collect()
df.select(split(df.s, '[ABC]', -1).alias('s')).collect()

#pyspark.sql.functions.substring:
df = spark.createDataFrame([('abcd',)], ['s',])
df.select(substring(df.s, 1, 2).alias('s')).collect()

#pyspark.sql.functions.substring_index:
df = spark.createDataFrame([('a.b.c.d',)], ['s'])
df.select(substring_index(df.s, '.', 2).alias('s')).collect()
df.select(substring_index(df.s, '.', -3).alias('s')).collect()

#pyspark.sql.functions.overlay:
df = spark.createDataFrame([("SPARK_SQL", "CORE")], ("x", "y"))
df.select(overlay("x", "y", 7).alias("overlayed")).collect()
df.select(overlay("x", "y", 7, 0).alias("overlayed")).collect()
df.select(overlay("x", "y", 7, 2).alias("overlayed")).collect()

#pyspark.sql.functions.sentences:
df = spark.createDataFrame([["This is an example sentence."]], ["string"])
df.select(sentences(df.string, lit("en"), lit("US"))).show(truncate=False)
df = spark.createDataFrame([["Hello world. How are you?"]], ["s"])
df.select(sentences("s")).show(truncate=False)

#pyspark.sql.functions.translate:
spark.createDataFrame([('translate',)], ['a']).select(translate('a', "rnlt", "123") \
alias('r')).collect()

#pyspark.sql.functions.trim:
df = spark.createDataFrame(["   Spark", "Spark  ", " Spark"], "STRING")
df.select(trim("value").alias("r")).withColumn("length", length("r")).show()

#pyspark.sql.functions.upper:
df = spark.createDataFrame(["Spark", "PySpark", "Pandas API"], "STRING")
df.select(upper("value")).show()

#pyspark.sql.functions.call_udf:
from pyspark.sql.functions import call_udf, col
from pyspark.sql.types import IntegerType, StringType
df = spark.createDataFrame([(1, "a"),(2, "b"), (3, "c")],["id", "name"])
_ = spark.udf.register("intX2", lambda i: i * 2, IntegerType())
df.select(call_udf("intX2", "id")).show()
_ = spark.udf.register("strX2", lambda s: s * 2, StringType())
df.select(call_udf("strX2", col("name"))).show()

#pyspark.sql.functions.pandas_udf:
import pandas as pd
from pyspark.sql.functions import pandas_udf

@pandas_udf(IntegerType())
def slen(s: pd.Series) -> pd.Series:
return s.str.len()

from pyspark.sql.functions import PandasUDFType
from pyspark.sql.types import IntegerType
@pandas_udf(IntegerType(), PandasUDFType.SCALAR)
def slen(s):
return s.str.len()

@pandas_udf("col1 string, col2 long")
def func(s1: pd.Series, s2: pd.Series, s3: pd.DataFrame) -> pd.DataFrame:
s3['col2'] = s1 + s2.str.len()
return s3
# Create a Spark DataFrame that has three columns including a struct column
df = spark.createDataFrame(
[[1, "a string", ("a nested string",)]],
"long_col long, string_col string, struct_col struct<col1:string>")
df.printSchema()
df.select(func("long_col", "string_col", "struct_col")).printSchema()

@pandas_udf("string")
def to_upper(s: pd.Series) -> pd.Series:
return s.str.upper()
df = spark.createDataFrame([("John Doe",)], ("name",))
df.select(to_upper("name")).show()

@pandas_udf("first string, last string")
def split_expand(s: pd.Series) -> pd.DataFrame:
return s.str.split(expand=True)
df = spark.createDataFrame([("John Doe",)], ("name",))
df.select(split_expand("name")).show()


from typing import Iterator
@pandas_udf("long")
def plus_one(iterator: Iterator[pd.Series]) -> Iterator[pd.Series]:
for s in iterator:
yield s + 1
df = spark.createDataFrame(pd.DataFrame([1, 2, 3], columns=["v"]))
df.select(plus_one(df.v)).show()

from typing import Iterator, Tuple
from pyspark.sql.functions import struct, col
@pandas_udf("long")
def multiply(iterator: Iterator[Tuple[pd.Series, pd.DataFrame]]) -> Iterator[pd.Series]:
for s1, df in iterator:
yield s1 * df.v
df = spark.createDataFrame(pd.DataFrame([1, 2, 3], columns=["v"]))
df.withColumn('output', multiply(col("v"), struct(col("v")))).show()

@pandas_udf("double")
def mean_udf(v: pd.Series) -> float:
return v.mean()
df = spark.createDataFrame(
[(1, 1.0), (1, 2.0), (2, 3.0), (2, 5.0), (2, 10.0)], ("id", "v"))
df.groupby("id").agg(mean_udf(df['v'])).show()

from pyspark.sql import Window
@pandas_udf("double")
def mean_udf(v: pd.Series) -> float:
return v.mean()
df = spark.createDataFrame(
[(1, 1.0), (1, 2.0), (2, 3.0), (2, 5.0), (2, 10.0)], ("id", "v"))
w = Window.partitionBy('id').orderBy('v').rowsBetween(-1, 0)
df.withColumn('mean_v', mean_udf("v").over(w)).show()

#pyspark.sql.functions.udf:
from pyspark.sql.types import IntegerType
import random
random_udf = udf(lambda: int(random.random() * 100), IntegerType()).asNondeterministic()

from pyspark.sql.types import IntegerType
slen = udf(lambda s: len(s), IntegerType())
@udf
def to_upper(s):
if s is not None:
return s.upper()
@udf(returnType=IntegerType())
def add_one(x):
if x is not None:
return x + 1
df = spark.createDataFrame([(1, "John Doe", 21)], ("id", "name", "age"))
df.select(slen("name").alias("slen(name)"), to_upper("name"), add_one("age")).show()

#pyspark.sql.functions.unwrap_udt:
#pyspark.sql.functions.md5:
spark.createDataFrame([('ABC',)], ['a']).select(md5('a').alias('hash')).collect()

#pyspark.sql.functions.sha1:
spark.createDataFrame([('ABC',)], ['a']).select(sha1('a').alias('hash')).collect()

#pyspark.sql.functions.sha2:
df = spark.createDataFrame([["Alice"], ["Bob"]], ["name"])
df.withColumn("sha2", sha2(df.name, 256)).show(truncate=False)

#pyspark.sql.functions.crc32:
spark.createDataFrame([('ABC',)], ['a']).select(crc32('a').alias('crc32')).collect()

#pyspark.sql.functions.hash:
df = spark.createDataFrame([('ABC', 'DEF')], ['c1', 'c2'])

df.select(hash('c1').alias('hash')).show()

df.select(hash('c1', 'c2').alias('hash')).show()

#pyspark.sql.functions.xxhash64:
df = spark.createDataFrame([('ABC', 'DEF')], ['c1', 'c2'])

df.select(xxhash64('c1').alias('hash')).show()

df.select(xxhash64('c1', 'c2').alias('hash')).show()

#pyspark.sql.functions.assert_true:
df = spark.createDataFrame([(0,1)], ['a', 'b'])
df.select(assert_true(df.a < df.b).alias('r')).collect()
df.select(assert_true(df.a < df.b, df.a).alias('r')).collect()
df.select(assert_true(df.a < df.b, 'error').alias('r')).collect()
df.select(assert_true(df.a > df.b, 'My error msg').alias('r')).collect()

#pyspark.sql.functions.raise_error:
df = spark.range(1)
df.select(raise_error("My error message")).show()

