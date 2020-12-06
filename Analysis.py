# Daniel Roberts
# Daoqun Yang

# CS 4371 Final Project
# This program is used to analize the data collected by
# Spark.py and Stream.py.

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd
import matplotlib.pyplot as plt

# Pyspark
# Creating session
spark = SparkSession.builder.master("local").getOrCreate()

# Reading data in
rdd = spark.read.json("/home/daniel/Desktop/4371 Proj/Stream/Output-*/part-00000", multiLine = True)

# Making a country column
udf1 = udf(lambda x: x.split(",")[len(x.split(","))-1], StringType())
rdd = rdd.withColumn("Country", udf1(rdd.add))
rdd = rdd.withColumn("Country", regexp_replace("Country", "United States of America", "USA"))
rdd = rdd.withColumn("Country", regexp_replace("Country", "United States", "USA"))
rdd = rdd.withColumn("Country", regexp_replace("Country", " USA", "USA"))


# Number of tweets per country
rdd.createOrReplaceTempView("rdd")
countCountry = spark.sql("SELECT rdd.Country AS country, count(rdd.Country) AS NUMCountry FROM rdd GROUP BY rdd.Country ORDER BY NUMCountry DESC LIMIT 10").toPandas()

# Plotting
countCountry.plot(kind="bar", legend = None)
plt.xlabel("Country", fontsize=18)
plt.ylabel("Count", fontsize=16)
plt.xticks(range(len(countCountry["country"])), countCountry["country"])
plt.show()

# Number of each sentiment
gdf = rdd.groupBy(rdd.sent)
countSent = gdf.agg({"sent": "count"}).toPandas()

# Plotting
countSent.plot(kind = "bar", legend = None)
plt.xlabel("Sentiment", fontsize=18)
plt.ylabel("Count", fontsize=16)
plt.xticks(range(3), ["positive", "negative", "neutral"])
plt.show()

# Average rate per country
gdf = rdd.groupBy(rdd.Country)
rateCountry = gdf.agg({"rate": "average"}).toPandas()

# Plotting
rateCountry.plot(kind = "bar", legend = None)
plt.xlabel("Country", fontsize=18)
plt.ylabel("Sentament Range (-1, 1)", fontsize=16)
plt.xticks(range(len(rateCountry["Country"])), rateCountry["Country"])
plt.show()
