#!/usr/bin/env python
# coding: utf-8

# Import the required libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# Step 1: Initialize a Spark session
spark = SparkSession.builder \
    .appName("Spotify_Read_Files") \
    .getOrCreate()

# Load the data of spotify-album_data_2023.csv
titanic_df = spark.read.format("parquet").load("/user/spark/vinish/titanic_data_sqoop/2024-12-10_11-30-37_titanic_parquet")

# null count in Cabin coulmn is 77%, we will drop the column as it is of less importance.
titanic_df=titanic_df.drop("Cabin")

# Calculate the survival count
survived_passangers = titanic_df.groupBy("Survived").count()

# Calculate the survival count for those who survived
survival_count = survived_passangers.where(col("Survived") == 1).select("count").collect()[0][0]

# Add the survival percentage as a new column
titanic_df = titanic_df.withColumn("%_survived", round(lit(survival_count) / 891 * 100, 2))

loan_df.repartition(1).write.mode("overwrite").option("header", "true").csv("/user/spark/vinish/titanic_data_sqoop/transformed_titanic_dataset")
