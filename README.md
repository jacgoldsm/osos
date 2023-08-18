# osos
With `osos`, write Pyspark code on top of Pandas. The goal of `osos` is simple: unify the API for big data and small data analysis. 

If you like the `pandas` API, you can write Pandas code with small data and [pyspark.pandas](https://spark.apache.org/docs/latest/api/python/getting_started/quickstart_ps.html) code with big data.
If, like me, you hate the Pandas API and prefer Pyspark, this is the project for you.

## Getting Started
```python
import pandas as pd
import osos.functions as F
from osos.dataframe import DataFrame
from osos.window import Window


# Load data in with Pandas
iris_pd = pd.read_csv('https://raw.githubusercontent.com/mwaskom/seaborn-data/master/iris.csv')
# Make an osos DataFrame
iris = DataFrame(iris_pd)
# manipulate it just like Pyspark
agg = (
  iris
    .withColumn("Sepal.Area", F.col("Sepal.Length") * F.col("Sepal.Width"))
    .filter(F.col("Sepal.Length") > 4.9)
    .orderBy("Sepal.Area")
    .withColumn("total_area_by_species", F.sum("Sepal.Area").over(Window.partitionBy("Species")))
    .withColumn("species", F.lower("species"))
    .groupBy("species")
    .agg(F.avg("Sepal.Length").alias("avg_sepal_length"), F.avg("Sepal.Area").alias("avg_sepal_area"))
)

print(iris)

```
