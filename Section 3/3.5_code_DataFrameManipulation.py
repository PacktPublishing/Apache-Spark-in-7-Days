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


# dataset as a list
data = [('patty', 'spring', 'baseball', 64),
        ('patty', 'autumn', 'soccer', 78),
        ('matty', 'autumn', 'hockey', 90),
        ('matty', 'spring', 'soccer', 64),
        ('cathy', 'spring', 'baseball', 100),
        ('cathy', 'autumn', 'hockey', 78),
        ('sandy', 'autumn', 'soccer', 50),
        ('joey', 'summer', 'soccer', 73),
        ('tammy', 'spring', 'soccer', 86),
        ('marley', 'autumn', 'hockey', 100)]


# In[7]:


# create an rdd
rdd = sc.parallelize(data)
rdd


# In[8]:


# create a dataframe from an rdd and name the columns
df = spark.createDataFrame(rdd, ['player', 'season', 'sport', 'ranking'])


# In[9]:


# display dataframe
df.show()


# In[10]:


# Show average (mean) ranking
df.agg(
    {'ranking': 'avg'}
).show()


# In[11]:


# Shoe the mean, min, and max for ranking
df.agg(
    F.mean(df.ranking).alias('mean'),
    F.min(df.ranking).alias('min'),
    F.max(df.ranking).alias('max')
).show()


# In[12]:


# Group by player, show mean of ranking and count (# of sports)
df.groupby('player').agg({'ranking': 'mean', 'sport': 'count'}).show()


# In[13]:


# create another dataset called meta
meta = [('patty', 'community', 25),
        ('matty', 'college', 35),
        ('cathy', 'community', 40),
        ('sandy', 'college', 60),
        ('joey', 'community', 55),
        ('tammy', 'college', 23),
        ('marley', 'community', 45)]


# In[14]:


# create schema
schema = StructType([
    StructField('player', StringType(), True),
    StructField('league', StringType(), True),
    StructField('age', IntegerType(), True)
])


# In[15]:


# create dataframe, using dataset and schema
df_meta = spark.createDataFrame(meta, schema)


# In[16]:


# print schema
df_meta.printSchema()


# In[17]:


# show dataframe
df_meta.show()


# In[18]:


# inner join on player, notice multiple entries
df.join(df_meta, on='player', how='inner').show()


# In[19]:


# right outer join
df_full = df.join(df_meta, on='player', how='rightouter')
df_full.show()


# In[20]:


# Use drop() to drop columns
df_full.drop()


# In[21]:


# Group by league, show average ranking and average age
df_full.groupby('league').mean().show()


# In[22]:


# Group by league, and pivot by sport. Show average age.
df_full.groupby('league').pivot('sport').agg(F.mean('age')).show()

# The only 2 baseball players were cathy and patty and both were in 
# community leagues so null shows up in baseball column first entry.


# In[ ]:




