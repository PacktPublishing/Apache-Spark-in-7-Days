#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# Example of Structured Streaming


# In[ ]:


# The key idea in Structured Streaming is to treat a live data stream 
# as a table that is being continuously appended. This leads to a new 
# stream processing model that is very similar to a batch processing model.

# start the Spark Context
import findspark
findspark.init()


# In[ ]:


import pyspark
from pyspark import SparkContext


# In[ ]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

spark = SparkSession     .builder     .appName("StructuredNetworkWordCount")     .master("local[*]")    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR") 


# In[ ]:


# Create DataFrame representing the stream of input lines from connection to localhost:9999
# This lines DataFrame represents an unbounded table containing 
# the streaming text data.
lines = spark     .readStream     .format("socket")     .option("host", "localhost")     .option("port", 9999)     .load()

# Split the lines into words
# Next, we have used two built-in SQL functions - split and explode, 
# to split each line into multiple rows with a word each.
words = lines.select(
   explode(
       split(lines.value, " ")
   ).alias("word")
)

# Generate running word count
wordCounts = words.groupBy("word").count()


# In[ ]:


# Before executing next cell, start netcat server in terminal with 'nc -lk 9999'
# run next cell below in this notebook
# enter in some text in netcat server terminal:
# 'apache spark', 'apache hadoop', 'apache spark', 'apache hadoop'
# 'structured streaming', 'structured streaming'


# In[ ]:


# Start running the query that prints the running counts to the console
query = wordCounts     .writeStream     .outputMode("complete")     .format("console")     .start()

# The query object is a handle to that active streaming query, and we 
# have decided to wait for the termination of the query using 
# awaitTermination() to prevent the process from exiting while the 
# query is active.
query.awaitTermination()


# In[ ]:





# In[ ]:




