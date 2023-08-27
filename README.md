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


print(iris)

```

The same process in Pandas looks a lot worse (to me):

```python
iris_pd = iris_pd.copy()
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

print(iris_pd)
```

## Contributing

Before making a pull request, run the tests to ensure that none of them are broken. You can do 
this with the following code on Unix (todo: Windows).
```bash
pip install pytest
python3 -m pytest
```

If you are contributing a feature (as opposed to a bugfix), add a test to tests/test_basic.py
to cover the new feature. Examples of new features are column methods in column.py 
(`Node.__add__`, `Node.alias`), DataFrame methods in DataFrames.py (`DataFrame.withColumn`, `DataFrame.agg`),
or functions in functions.py (`functions.sum`, `functions.cosh`). 

Note that to add a function, you must add both an abstract function to the public API in functions.py *and* an implementation in _implementations.py. The name of the function in _implementations.py must be 
{name}_func, e.g. functions.sum_distinct would be called sum_distinct_func in _implementations.py. The functions in functions.py will return `Func` nodes, and the functions in _implementations.py will take in and return pandas Series (or SeriesGroupBy) objects.

The semantics of osos should hew as closely as possible to the semantics of Pyspark, since osos is intended to implement the pyspark.sql API. When in doubt about the behavior of a Pyspark function, consult the [documentation](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/index.html). Of course, not all of the behavior of Pyspark is relevant to osos, since Pyspark is built on top of Apache Spark and not Pandas. 

Even for the functions and methods that are directly comparable, it is impossible for the semantics of osos to match Pyspark exactly. For example, all Pyspark data types are nullable, whereas there is no NULL data type in Pandas (object columns can contain None, and float columns can contain NaN). Exact compatibility of behavior with Pyspark is on a best effort basis and deviations should only happen if there is a fundamental way that Pyspark differs from Pandas that can't be easily corrected, like the NULL semantics.

### Typing
I'm gradually trying to get the typing correct for this project, but that's a work in progress. Turn on type checking if you are contributing to the project and try not to add any additional type errors, but getting the type checking right is a task for future me. Also, Pandas has some mistakes in their typing that we have to manually ignore here.
