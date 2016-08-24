"""
Numerical summarizations usually are of the form - finding the count (famous hello word count example of MapRedude), min, max of a value.
Here I try implementing that in Spark.
Given the 311 data - give me the first date (min), last date (max), and count of complaints per borough in NYC
"""


import os,sys
import numpy as np
import csv
import collections
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark import SparkContext, HiveContext
from operator import add
from pyspark.sql.types import DateType
from pyspark.sql.functions import col,udf, unix_timestamp
from pyspark.sql import *


if __name__ == "__main__":
        sc = SparkContext(appName = "test")
        sqlContext = SQLContext(sc)
        rdd = sc.textFile("311data.csv",use_unicode=False)
        rdd = rdd.mapPartitions(lambda x: csv.reader(x))
        rdd = rdd.filter(lambda x: x[0]!='Unique Key') # headers become rows - so either remove headers before using the data or remove them
        cols = ["Key","Date","Agency","Borough"] # Create column names
        df = sqlContext.createDataFrame(rdd,cols) # Convert rdds to dataframe
        func =  udf (lambda x: datetime.strptime(x.split(" ")[0], "%M/%d/%Y"), DateType()) # the Date is in String format - convert it to Date format
        # udf stands for user defined functions
        df = df.withColumn("Date", func(col("Date"))) # New Date column will have date format
        sqlContext.registerDataFrameAsTable(df, "table1") # You can aggregate data on the dataframe also, but I wanted to play with sql tables
        results = sqlContext.sql("SELECT min(Date),max(Date),count(*),Borough from table1 group by Borough")
        # with one single query you can get the desired output
        results_rdd = results.rdd
        results_rdd.saveAsTextFile("output")
