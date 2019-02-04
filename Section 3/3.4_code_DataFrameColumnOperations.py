#!/usr/bin/env python
# coding: utf-8

# In[1]:


# start the Spark Context
import findspark
findspark.init()


# In[2]:


import pyspark # only run after findspark.init()


# In[3]:


from pyspark.sql import SparkSession
sc = pyspark.SparkContext(appName="col")


# In[4]:


spark = SparkSession.builder.getOrCreate()


# In[5]:


import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


# In[6]:


data = [('patty', 'spring', 'baseball', 64),
        ('matty', 'autumn', 'hockey', 90),
        ('cathy', 'spring', 'baseball', 100),
        ('sandy', 'autumn', 'soccer', 50),
        ('joey', 'summer', 'soccer', 73),
        ('tammy', 'spring', 'soccer', 86),
        ('marley', 'autumn', 'hockey', 100)]


# In[7]:


# create an rdd
rdd = sc.parallelize(data)


# In[8]:


# take first 4 elements
rdd.take(4)


# In[9]:


# create a dataframe from an rdd and name the columns
df = spark.createDataFrame(rdd, ['player', 'season', 'sport', 'ranking'])


# In[10]:


df.show()


# In[11]:


# show the first 4 elements
df.show(4)


# In[12]:


# take the header of the dataframe
df.head()


# In[13]:


# count the number of elements in the dataframe
df.count()


# In[14]:


# describe the dataframe
df.describe().show()


# In[15]:


# print schema
df.printSchema()


# In[16]:


# select columns
df.select(['player', 'sport', 'ranking']).show()


# In[17]:


# filter where values are greater than 85 in rows of column 'ranking'
df.filter(df['ranking'] > 85).show()


# In[18]:


# filter where the season is autumn
df.filter(df['season'] == 'autumn').show()


# In[19]:


# select columns and create a new one
df.select(df['player'], df['season'], df['sport'], df['ranking'],
          (df['ranking'] - 15).alias('new_ranking')).show()


# In[20]:


# use withColumn() to create a new column
df.withColumn('lowered_ranking', df['ranking']* 0.33).show()


# In[21]:


# sort values in column
df.sort(df['ranking']).show()


# In[22]:


# sort values in column in descending order
df.sort(df['ranking'].desc()).show()


# In[23]:


# a different way to write the same code as previous
df.sort(df.ranking.desc()).show()  


# In[ ]:




