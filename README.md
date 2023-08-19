# osos
With `osos`, write Pyspark code on top of Pandas. The goal of `osos` is simple: unify the API for big data and small data analysis. 

If you like the `pandas` API, you can write Pandas code with small data and [pyspark.pandas](https://spark.apache.org/docs/latest/api/python/getting_started/quickstart_ps.html) code with big data.
If, like me, you hate the Pandas API and prefer Pyspark, this is the project for you.

## Getting Started
```python
import pandas as pd
import numpy as np
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
    .withColumn("sepal_area", F.col("sepal_length") * F.col("sepal_width"))
    .filter(F.col("sepal_length") > 4.9)
    .withColumn("total_area_by_species", F.sum("sepal_area").over(Window.partitionBy("species")))
    .withColumn("species", F.lower("species"))
    .groupBy("species")
    .agg(F.avg("sepal_length").alias("avg_sepal_length"), F.avg("sepal_area").alias("avg_sepal_area"))
)

print(iris)

```

The same process in Pandas looks a lot worse (to me):

```python
iris_pd = iris_pd.copy()
iris_pd['sepal_area'] = iris_pd['sepal_length'] * iris_pd['sepal_width']
iris_pd = iris_pd[iris_pd['sepal_length'] > 4.9]
iris_pd['total_area_by_species'] = iris_pd.groupby("species")['sepal_area'].transform(np.sum)
iris_pd['species'] = iris_pd['species'].str.lower()
iris_pd = iris_pd.groupby("species").agg(np.average).reset_index()[["species", "sepal_length", "sepal_area"]]
iris_pd = iris_pd.rename(columns={"sepal_length":"avg_sepal_length", "sepal_area":"avg_sepal_area"})

print(iris_pd)
```
