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


# In[3]:


sc = spark.sparkContext


# In[4]:


from pyspark.sql import Row
from pyspark.sql import HiveContext
sqlContext = HiveContext(sc)


# In[5]:


# sample data
cat_list = [('Tigress', 25),('Karrot', 33),('Dakota', 17),('George', 19)]


# In[6]:


# parallelize to create rdd
rdd = sc.parallelize(cat_list)


# In[7]:


# transform RDD, using Row function
cats = rdd.map(lambda x: Row(name=x[0], age=int(x[1])))


# In[8]:


cats


# In[9]:


# Create dataframe
schemaCats = sqlContext.createDataFrame(cats)


# In[10]:


# Register it as a temp table
sqlContext.registerDataFrameAsTable(schemaCats, "cat_table")
# Show HIVE table
sqlContext.sql("show tables").show()


# In[11]:


# Using default HiveContext to select columns
sqlContext.sql("Select * from cat_table").show()


# In[12]:


# USE where clause
sqlContext.sql("Select * from cat_table where age > 20").show()


# In[13]:


sc.stop()
spark.stop()


# In[ ]:




