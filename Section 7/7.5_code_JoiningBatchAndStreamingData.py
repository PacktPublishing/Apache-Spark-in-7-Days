#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# Example of Joining batch and streaming data


# In[1]:


import findspark
findspark.init()


# In[2]:


import pyspark
from pyspark import SparkContext


# In[3]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.types import *


# In[4]:


spark = SparkSession         .builder         .appName("Joining")        .getOrCreate()


# In[5]:


# schema for the batch data
batch_schema = StructType([StructField('Traveler_ID', StringType(), True),                                     StructField('Vacation_Spot', StringType(), True),                                     StructField('Travel_Rewards', StringType(),True)])


# In[6]:


# read in the batch data
travelerDF = spark.read                    .format("csv")                    .option("header", "true")                    .schema(batch_schema)                    .load("VacationDatasets/batch_vacation_data/batch_data.csv")


# In[7]:


# schema for the streaming data
streaming_schema = StructType([StructField('Traveler_ID', StringType(), True),                                     StructField('Travel_Expense', StringType(), True),                                     StructField('Vacation_Rating', StringType(),True)])


# In[8]:


# read in the streaming data
fileStreamDf = spark.readStream                .option("header", "true")                .option("maxFilesPerTrigger",1)                .schema(streaming_schema)                .csv("VacationDatasets/streaming_vacation_data")


# In[9]:


# Join batch and streaming data
joinedDF = travelerDF.join(fileStreamDf, "Traveler_ID")


# In[ ]:


# write the output out to console
# notice that we use writeStream as output is a streaming data
query = joinedDF        .writeStream        .outputMode('append')        .format('console')        .start()

query.awaitTermination()        


# In[ ]:




