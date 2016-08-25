"""
Numerical summarizations part 2: Creating inverted index for querying.
Here we create an index on the Agency. To see which agency had a related complain - on which day and its count.
See results image to see what we get


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
        inverted_ind = df.groupBy(df.Agency, "Date").count()
        inverted_ind_rdd = inverted_ind.rdd
        # with one single query you can get the desired output
        results = inverted_ind_rdd.map(lambda x: (x[0],x[1:])).groupByKey().map(lambda x: (x[0], list(x[1])))
        #see results on prompt as t.take(1)
        # We have to do a second map as it returns an pyspark iterable (without the second map - hiding the results)
        # it means that we can see using list - for which we use the second map function after grouping
        results.saveAsTextFile("output")
