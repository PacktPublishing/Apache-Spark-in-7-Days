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


# Principal Component Analysis (PCA)

# In[3]:


# PCA is a statistical procedure that uses an orthogonal transformation 
# to convert a set of observations of possibly correlated variables 
# into a set of values of linearly uncorrelated variables called 
# principal components.
from pyspark.ml.feature import PCA
from pyspark.ml.linalg import Vectors


# In[4]:


data = [(Vectors.sparse(5, [(1, 1.0), (3, 7.0)]),),
        (Vectors.dense([2.0, 0.0, 3.0, 4.0, 5.0]),),
        (Vectors.dense([4.0, 0.0, 0.0, 6.0, 7.0]),)]


# In[5]:


df = spark.createDataFrame(data, ["features"])


# In[6]:


df.show()


# In[7]:


# The example below shows how to project 5-dimensional feature vectors into 
# 3-dimensional principal components.
pca = PCA(k=3, inputCol="features", outputCol="pcaFeatures")
model = pca.fit(df)


# In[8]:


result = model.transform(df).select("pcaFeatures")
result.show(truncate=False)


# In[ ]:


# end of section for Principal Component Analysis (PCA)


# One Hot Encoding  (OneHotEncoderEstimator)

# In[9]:


# One hot encoding is a process by which categorical variables are converted into a form that could 
# be provided to ML algorithms to do a better job in prediction.
from pyspark.ml.feature import OneHotEncoderEstimator


# In[10]:


df = spark.createDataFrame([
    (0.0, 1.0),
    (1.0, 0.0),
    (2.0, 1.0),
    (0.0, 2.0),
    (0.0, 1.0),
    (2.0, 0.0)
], ["categoryIndex1", "categoryIndex2"])


# In[11]:


df.show()


# In[12]:


encoder = OneHotEncoderEstimator(inputCols=["categoryIndex1", "categoryIndex2"],
                                 outputCols=["categoryVec1", "categoryVec2"])


# In[13]:


encoder


# In[14]:


model = encoder.fit(df)


# In[15]:


model


# In[16]:


encoded = model.transform(df)


# In[17]:


encoded.show()


# In[ ]:


# end of section on One Hot Encoder


# Min Max Scaler (MinMaxScaler)

# In[18]:


# MinMaxScaler transforms a dataset of Vector rows, rescaling each 
# feature to a specific range (often [0, 1]). 
# min: 0.0 by default. Lower bound after transformation, shared by all features.
# max: 1.0 by default. Upper bound after transformation, shared by all features
from pyspark.ml.feature import MinMaxScaler
from pyspark.ml.linalg import Vectors


# In[19]:


dataFrame = spark.createDataFrame([
    (0, Vectors.dense([1.0, 0.1, -1.0]),),
    (1, Vectors.dense([2.0, 1.1, 1.0]),),
    (2, Vectors.dense([3.0, 10.1, 3.0]),)
], ["id", "features"])


# In[20]:


dataFrame.show()


# In[21]:


scaler = MinMaxScaler(inputCol="features", outputCol="scaledFeatures")


# In[22]:


scaler


# In[23]:


# Compute summary statistics and generate MinMaxScalerModel
scalerModel = scaler.fit(dataFrame)


# In[24]:


scalerModel


# In[25]:


# rescale each feature to range [min, max].
scaledData = scalerModel.transform(dataFrame)
print("Features scaled to range: [%f, %f]" % (scaler.getMin(), scaler.getMax()))


# In[26]:


scaledData.select("features", "scaledFeatures").show()


# In[27]:


spark.stop()


# In[ ]:




