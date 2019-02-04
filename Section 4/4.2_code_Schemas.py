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

from pyspark.sql import Row

sc = spark.sparkContext


# In[3]:


lines = sc.textFile('people.txt')


# In[4]:


# Inferring the Schema Using Reflection
parts = lines.map(lambda l: l.split(","))

# Spark SQL can convert an RDD of Row objects to a DataFrame, inferring the datatypes.
# Rows are constructed by passing a list of key/value pairs as kwargs to the Row class. 
# The keys of this list define the column names of the table, and the types are inferred 
# by sampling the whole dataset, similar to the inference that is performed on JSON files.
people = parts.map(lambda p: Row(name=p[0], age=int(p[1])))


# In[5]:


# Infer the schema, and register the DataFrame as a table.
schemaPeople = spark.createDataFrame(people)
schemaPeople.createTempView("people")


# In[6]:


# SQL can be run over DataFrames that have been registered as a table.
teenagers = spark.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19")


# In[7]:


teenagers


# In[8]:


# The results of SQL queries are Dataframe objects.
# rdd returns the content as an :class:`pyspark.RDD` of :class:`Row`.
teenNames = teenagers.rdd.map(lambda p: 'Name: ' + p.name)


# In[9]:


teenNames.collect()


# In[10]:


# Alternate method:  Programmatically Specifying the Schema
# Import data types
from pyspark.sql.types import *

# sc = spark.sparkContext


# In[11]:


# Load a text file and convert each line to a Row.
lines2 = sc.textFile('people.txt')


# In[12]:


parts2 = lines2.map(lambda l: l.split(","))

# Each line is converted to a tuple.
people2 = parts2.map(lambda p: (p[0], p[1].strip()))


# In[13]:



# The schema is encoded in a string.
schemaString = "name age"


# In[14]:


# Create schema
fields2 = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
schema2 = StructType(fields2)


# In[15]:


# Apply the schema to the RDD.
schemaPeople2 = spark.createDataFrame(people2, schema2)

# Creates a temporary view using the DataFrame
schemaPeople2.createTempView("persons")


# In[16]:



# SQL can be run over DataFrames that have been registered as a table.
results = spark.sql("SELECT name FROM persons")


# In[17]:


# show results
results.show()


# In[18]:


spark.stop()
sc.stop()


# In[ ]:




