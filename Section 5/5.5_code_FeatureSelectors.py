#!/usr/bin/env python
# coding: utf-8

# In[1]:


# start the Spark Context
import findspark
findspark.init()


# In[2]:


import pyspark # only run after findspark.init()
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()


# Vector Slicer (VectorSlicer)

# In[3]:


# VectorSlicer is a transformer that takes a feature vector and outputs a 
# new feature vector with a sub-array of the original features. It is 
# useful for extracting features from a vector column.
# VectorSlicer accepts a vector column with specified indices, then 
# outputs a new vector column whose values are selected via those indices. 
from pyspark.ml.feature import VectorSlicer
from pyspark.ml.linalg import Vectors
from pyspark.sql.types import Row


# In[4]:


df = spark.createDataFrame([
    Row(userFeatures=Vectors.sparse(3, {0: -2.0, 1: 2.3})),
    Row(userFeatures=Vectors.dense([-2.0, 2.3, 0.0]))])


# In[5]:


df.show()


# In[6]:


# Vector Slicer slices the vector to select only specified indices.
# VectorSlicer takes a vector as input column and create a new vector 
# which contain only part of the attributes of the original vector.
slicer = VectorSlicer(inputCol="userFeatures", 
                      outputCol="features", indices=[1])


# In[7]:


output = slicer.transform(df)


# In[8]:



output.select("userFeatures", "features").show()


# In[ ]:


# end of section on Vector Slicer


# Chi Squared Selector (ChiSqSelector)

# In[9]:


# ChiSqSelector stands for Chi-Squared feature selection. 
# It operates on labeled data with categorical features.
# ChiSqSelector uses the Chi-Squared test of independence to 
# decide which features to choose.
from pyspark.ml.feature import ChiSqSelector
from pyspark.ml.linalg import Vectors


# In[10]:


df = spark.createDataFrame([
    (7, Vectors.dense([0.0, 0.0, 18.0, 1.0]), 1.0,),
    (8, Vectors.dense([0.0, 1.0, 12.0, 0.0]), 0.0,),
    (9, Vectors.dense([1.0, 0.0, 15.0, 0.1]), 0.0,)], 
    ["id", "features", "clicked"])


# In[11]:


df.show()


# In[12]:


selector = ChiSqSelector(numTopFeatures=1, featuresCol="features",
                         outputCol="selectedFeatures", labelCol="clicked")


# In[13]:


result = selector.fit(df).transform(df)


# In[14]:


print("ChiSqSelector output with top %d features selected" 
      % selector.getNumTopFeatures())
result.show()


# In[15]:


# stop spark session
spark.stop()


# In[ ]:




