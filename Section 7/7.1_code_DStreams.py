#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# Example 1


# In[ ]:


# start the Spark Context
import findspark
findspark.init()


# In[ ]:


import pyspark 


# In[ ]:


from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# Create a local StreamingContext with two working thread and 
# batch interval of 1 second
sc = SparkContext("local[2]", "NetworkWordCount")
ssc = StreamingContext(sc, 1)


# In[ ]:


# This lines DStream represents the stream of data that will be 
# received from the data server. Each record in this DStream is a 
# line of text.

# Create a DStream that will connect to 
# hostname:port, like localhost:9999
lines = ssc.socketTextStream("localhost", 9999)


# In[ ]:


# flatMap is a one-to-many DStream operation that creates a new DStream by
# generating multiple new records from each record in the source DStream. 
# In this case, each line will be split into multiple words and the stream 
# of words is represented as the words DStream.

# Split each line into words
words = lines.flatMap(lambda line: line.split(" "))


# In[ ]:


# Transformations yield a new DStream from a previous one.
# Count each word in each batch
pairs = words.map(lambda word: (word, 1))
wordCounts = pairs.reduceByKey(lambda x, y: x + y)

# Print the first ten elements of each RDD generated in this DStream 
wordCounts.pprint()


# In[ ]:


# In the terminal, run a Netcat as a data server
# $ nc -lk 9999
# Then, input any words you want with some repetition amongst the words.


# Note that when these lines are executed, Spark Streaming only sets up 
# the computation it will perform when it is started, and no real 
# processing has started yet.

ssc.start()             # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate


# In[ ]:


ssc.stop()


# In[ ]:





# In[ ]:


# Example 2


# In[1]:


# start the Spark Context
import findspark
findspark.init()


# In[2]:


import pyspark 


# In[3]:


from operator import add, sub
from time import sleep
from pyspark import SparkContext
from pyspark.streaming import StreamingContext


# In[4]:


# Set up the Spark context and the streaming context
sc = SparkContext(appName="StreamExample")
ssc = StreamingContext(sc, 1)


# In[ ]:


# stop the kernel
# Control + C in terminal for Netcat server


# In[5]:


# Queue of RDDs as a Stream
# For testing a Spark Streaming application with test data, one can also 
# create a DStream based on a queue of RDDs, using 
# streamingContext.queueStream(queueOfRDDs). Each RDD pushed into the 
# queue will be treated as a batch of data in the DStream, and processed 
#like a stream.

# Input data
rddQueue = []
for x in range(5):
    rddQueue += [ssc.sparkContext.parallelize([x, x+1])]


# In[6]:


inputStream = ssc.queueStream(rddQueue)


# In[7]:


inputStream.map(lambda x: "Input: " + str(x)).pprint()

# perform addition with reduce function
# pprint() is not lazy and should compute right away
inputStream.reduce(add)    .map(lambda x: "Output: " + str(x))    .pprint()


# In[8]:


ssc.start()
sleep(5)
ssc.stop(stopSparkContext=True, stopGraceFully=True)


# In[9]:


ssc.stop()


# In[ ]:




