#!/usr/bin/env python
# coding: utf-8

# In[1]:


import findspark
findspark.init()


# In[2]:


from pyspark.sql import SparkSession

spark = SparkSession     .builder     .appName("Python Spark SQL basic example")     .config("spark.some.config.option", "some-value")     .getOrCreate()


# In[3]:


# spark is an existing SparkSession
df = spark.read.json("people.json")
# Displays the content of the DataFrame to stdout
df.show()


# In[4]:


# spark, df are from the previous example
# Print the schema in a tree format
df.printSchema()


# In[5]:


# Select only the "name" column
df.select("name").show()


# In[6]:


# Select everybody, but increment the age by 1
df.select(df['name'], df['age'] + 1).show()


# In[7]:


# Count people by age
df.groupBy("age").count().show()


# In[8]:



# The sql function on a SparkSession enables applications to run 
# SQL queries programmatically and returns the result as a DataFrame.

# Register the DataFrame as a SQL temporary view
df.createOrReplaceTempView("people")

# Temporary views in Spark SQL are session-scoped and will disappear 
# if the session that creates it terminates.
sqlDF = spark.sql("SELECT * FROM people")
sqlDF.show()


# In[9]:


# If you want to have a temporary view that is shared among all sessions 
# and keep alive until the Spark application terminates, you can create 
# a global temporary view. Global temporary view is tied to a system 
# preserved database global_temp .

# Register the DataFrame as a global temporary view
df.createGlobalTempView("people")


# In[10]:


# Global temporary view is tied to a system preserved database `global_temp`
spark.sql("SELECT * FROM global_temp.people").show()


# In[11]:


# Global temporary view is cross-session
spark.newSession().sql("SELECT * FROM global_temp.people").show()


# In[12]:


spark.stop()


# In[ ]:




