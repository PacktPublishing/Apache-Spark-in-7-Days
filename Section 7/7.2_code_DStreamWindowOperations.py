#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# Example 1


# In[ ]:


# updateStateByKey(func)
# Return a new "state" DStream where the state for each key is 
# updated by applying the given function on the previous state of 
# the key and the new values for the key. This can be used to maintain 
# arbitrary state data for each key.

# start the Spark Context
import findspark
findspark.init()
import pyspark

from pyspark import SparkContext

from pyspark.streaming import StreamingContext

sc = SparkContext("local[2]", "window")
ssc = StreamingContext(sc, 30)
ssc.checkpoint("checkpoint")
def updateFunc(new_values, last_sum):
    return sum(new_values) + (last_sum or 0)
    
lines = ssc.socketTextStream('localhost', 9999)
wordcounts = lines.map(lambda word: (word, 1))                   .updateStateByKey(updateFunc)
    
wordcounts.pprint()
ssc.start()
ssc.awaitTermination()

# open up a terminal, and start netcat server   nc -lk 9999
# use 'Lorem ipsum dolor' <pause for 30 seconds> and repeat text again 2 or 3 times
# Use Control + c in terminal to stop netcat server
# Use Kernel (interrupt) in the notebook

# stop
ssc.stop(stopSparkContext=True, stopGraceFully=True)

# Output
# -------------------------------------------
# Time: 2019-01-23 11:09:30
# -------------------------------------------

# -------------------------------------------
# Time: 2019-01-23 11:10:00
# -------------------------------------------
# ('Lorem ipsum dolor', 1)

# -------------------------------------------
# Time: 2019-01-23 11:10:30
# -------------------------------------------
# ('Lorem ipsum dolor', 2)

# -------------------------------------------
# Time: 2019-01-23 11:11:00
# -------------------------------------------
# ('Lorem ipsum dolor', 3)


# In[ ]:





# In[ ]:


# Example 2 


# In[ ]:


# window(windowLength, slideInterval) 
# Return a new DStream which is computed based on windowed batches.

# start the Spark Context
import findspark
findspark.init()
import pyspark

from time import sleep
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

sc = SparkContext(appName="window")
ssc = StreamingContext(sc, 1)

Users = [
    ["Alice", "Bob"],
    ["Bob"],
    ["Carlos", "Dan"],
    ["Carlos", "Dan", "Erin"],
    ["Carlos", "Frank"],
]

rddQueue = []
for datum in Users:
    rddQueue += [ssc.sparkContext.parallelize(datum)]    
    


inputStream = ssc.queueStream(rddQueue)

# window length is 5 and sliding interval is 1
inputStream.window(5, 1)           .map(lambda x: set([x]))           .reduce(lambda x, y: x.union(y))           .pprint()    

ssc.start()
sleep(5)
ssc.stop(stopSparkContext=True, stopGraceFully=True)

# Output

# -------------------------------------------
# Time: 2019-01-23 11:12:46
# -------------------------------------------
# {'Alice', 'Bob'}

# -------------------------------------------
# Time: 2019-01-23 11:12:47
# -------------------------------------------
# {'Alice', 'Bob'}

# -------------------------------------------
# Time: 2019-01-23 11:12:48
# -------------------------------------------
# {'Carlos', 'Alice', 'Dan', 'Bob'}

# -------------------------------------------
# Time: 2019-01-23 11:12:49
# -------------------------------------------
# {'Erin', 'Dan', 'Alice', 'Bob', 'Carlos'}

# -------------------------------------------
# Time: 2019-01-23 11:12:50
# -------------------------------------------
# {'Erin', 'Frank', 'Dan', 'Alice', 'Bob', 'Carlos'}

# -------------------------------------------
# Time: 2019-01-23 11:12:51
# -------------------------------------------
# {'Erin', 'Frank', 'Dan', 'Bob', 'Carlos'}

# -------------------------------------------
# Time: 2019-01-23 11:12:52
# -------------------------------------------
# {'Erin', 'Frank', 'Carlos', 'Dan'}


# In[ ]:





# In[ ]:


# Example 3


# In[ ]:


# reduceByKeyAndWindow(func, 
#                      InvFunc, 
#                      windowLength,
#                      slideInterval) 

# start the Spark Context
import findspark
findspark.init()
import pyspark

from time import sleep
from pyspark import SparkContext

sc = SparkContext(appName="windowed")
from pyspark.streaming import StreamingContext
ssc = StreamingContext(sc, 5)
ssc.checkpoint("checkpoint")

# reduceByKeyAndWindow() 
# When called on a DStream of (K, V) pairs, returns a new DStream of 
# (K, V) pairs where the values for each key are aggregated using the 
#given reduce function func over batches in a sliding window. window. 

lines = ssc.socketTextStream('localhost', 9999)
windowedWordCounts = lines.map(lambda word: (word, 1))                           .reduceByKeyAndWindow(lambda x, y: x + y,                              lambda x, y: x - y, 30, 10)
## window length = 30
## sliding interval = 10

        
windowedWordCounts.pprint()
ssc.start()
ssc.awaitTermination()
ssc.stop(stopSparkContext=True, stopGraceFully=True)


# start netcat server with nc -lk 9999

# Enter these 3 lines (copy and paste) and wait
# lorem ipsum dolor
# lorem ipsum dolor
# lorem ipsum dolor

# Then enter 4 lines (copy and paste)
# lorem ipsum dolor
# lorem ipsum dolor
# lorem ipsum dolor
# lorem ipsum dolor

# output:
#-------------------------------------------
#Time: 2019-01-23 11:04:55
#-------------------------------------------

#-------------------------------------------
#Time: 2019-01-23 11:05:05
#-------------------------------------------
#('lorem ipsum dolor', 3)

#-------------------------------------------
#Time: 2019-01-23 11:05:15
#-------------------------------------------
#('lorem ipsum dolor', 7)

#-------------------------------------------
#Time: 2019-01-23 11:05:25
#-------------------------------------------
#('lorem ipsum dolor', 7)

#-------------------------------------------
#Time: 2019-01-23 11:05:35
#-------------------------------------------
#('lorem ipsum dolor', 4)


# In[ ]:





# In[ ]:




