#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# Example of using Structured Streaming Window Operations


# In[ ]:


import findspark
findspark.init()


# In[ ]:


import pyspark
from pyspark import SparkContext


# In[ ]:


from pyspark.sql import Row, SparkSession
from pyspark.sql.streaming import DataStreamWriter, DataStreamReader
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.functions import window


# In[ ]:


spark = SparkSession.builder                     .appName("WindowedCount")                     .master("local[*]")                     .getOrCreate() 
spark.sparkContext.setLogLevel("ERROR") 


# In[ ]:


WindowSize = '40 seconds'
SlidingInterval = '20 seconds'


# In[ ]:


# Set up the `lines` readStream to take in data from a socket stream, AND include the timestamp
lines = spark.readStream.format("socket")             .option("host", "localhost")             .option("port", 9999)              .option('includeTimestamp', 'true')              .load()


# In[ ]:



# Splitting the lines into words
words = lines.select(explode(split(lines.value, " "))              .alias("word"),lines.timestamp)


# In[ ]:


# Add a watermark to the data, using the timestamp
words = words.withWatermark("timestamp", "5 seconds")


# In[ ]:


# Write out the windowed wordcounts using groupBy(), window(), and count().
windowedCounts = words.groupBy(window(words.timestamp,                       WindowSize, SlidingInterval),words.word).count()


# In[ ]:


# Before executing this cell, start netcat server 'nc -lk 9999' 
# and have it ready to input your own text
# 'Hello World'
# 'Apache Spark'
# 'Spark is awesome'
# 'Structured Streaming
# 'Window Operations'

query = windowedCounts.writeStream.outputMode("complete").option("numRows","1000").option("truncate","false")                    .format("console").start()

query.awaitTermination()


# In[ ]:




