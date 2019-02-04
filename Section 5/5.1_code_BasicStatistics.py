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


# Correlation

# In[3]:


# Calculating the correlation between two series of data is a common 
# operation in Statistics. In spark.ml we provide the flexibility to 
# calculate pairwise correlations among many series. The supported 
#correlation methods are currently Pearson’s and Spearman’s correlation.
from pyspark.ml.linalg import Vectors
from pyspark.ml.stat import Correlation


# In[4]:


# Create a list of sparse and dense vectors.
data = [(Vectors.sparse(4, [(0, 1.0), (3, -2.0)]),),
        (Vectors.dense([4.0, 5.0, 0.0, 3.0]),),
        (Vectors.dense([6.0, 7.0, 0.0, 8.0]),),
        (Vectors.sparse(4, [(0, 9.0), (3, 1.0)]),)]


# In[5]:


data


# In[6]:


# create dataframe with data and features as inputs
df = spark.createDataFrame(data, ["features"])


# In[7]:


df.show()


# In[8]:


# Correlation computes the correlation matrix for the input Dataset of 
# Vectors using the specified method. The output will be a DataFrame 
# that contains the correlation matrix of the column of vectors.

# Pearson Correlation
r1 = Correlation.corr(df, "features").head()
print("Pearson correlation matrix:\n" + str(r1[0]))


# In[9]:


# Spearman correlation
r2 = Correlation.corr(df, "features", "spearman").head()
print("Spearman correlation matrix:\n" + str(r2[0]))


# In[ ]:


# end of Correlation section


# Hypothesis Testing

# In[10]:


# Hypothesis testing is a powerful tool in statistics to determine 
# whether a result is statistically significant, whether this result 
# occurred by chance or not.  The null hypothesis is no effect.

from pyspark.ml.linalg import Vectors
from pyspark.ml.stat import ChiSquareTest


# In[11]:


data = [(0.0, Vectors.dense(0.5, 10.0)),
        (0.0, Vectors.dense(1.5, 20.0)),
        (1.0, Vectors.dense(1.5, 30.0)),
        (0.0, Vectors.dense(3.5, 30.0)),
        (0.0, Vectors.dense(3.5, 40.0)),
        (1.0, Vectors.dense(3.5, 40.0))]


# In[12]:


data


# In[13]:


df = spark.createDataFrame(data, ["label", "features"])


# In[14]:


# goodness of fit, measure of relationship
# ChiSquareTest conducts Pearson’s independence test for every feature 
# against the label. For each feature, the (feature, label) pairs are 
# converted into a contingency matrix for which the Chi-squared statistic 
# is computed. All label and feature values must be categorical.

r = ChiSquareTest.test(df, "features", "label").head()


# In[15]:


r


# In[16]:


print("pValues: " + str(r.pValues))
print("degreesOfFreedom: " + str(r.degreesOfFreedom))
print("statistics: " + str(r.statistics))


# In[17]:


spark.stop()


# In[ ]:




