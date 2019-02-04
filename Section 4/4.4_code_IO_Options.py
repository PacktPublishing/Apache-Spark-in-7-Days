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


# Load JSON file
# You can also manually specify the data source that will be used along 
#with any extra options that you would like to pass to the data source.
df4 = spark.read.load("people.json", format="json")

# Select columns and write as parquet file
df4.select("name","age").write.save("namesAndAges.parquet", 
                                    format="parquet")


# In[5]:


# Load csv file
df5 = spark.read.load("people.csv",format="csv", sep=",", 
                     inferSchema="true", header="true")


# In[6]:


df5.show()


# In[7]:


# Run SQL on files directly
# Instead of using read API to load a file into a DF and query it,
# you can also query that file directly with SQL.
df6 = spark.sql("SELECT * FROM parquet.`users.parquet`")


# In[8]:


df6.show()


# In[9]:


peopleDF = spark.read.json("people.json")

# DataFrames can be saved as Parquet files, maintaining the schema information.
peopleDF.write.parquet("people.parquet")

# Read in the Parquet file created above.
# Parquet files are self-describing so the schema is preserved.
# The result of loading a parquet file is also a DataFrame.
parquetFile = spark.read.parquet("people.parquet")

# Parquet files can also be used to create a temporary view 
# and then used in SQL statements.

parquetFile.createOrReplaceTempView("parquetFile")
teenagers = spark.sql("SELECT name FROM parquetFile WHERE age >= 13 AND age <= 19")
teenagers.show()


# In[10]:


# Schema Merging
# Parquet supports schema evolution.  Users can starts with a simple schema
# and then add more columns to the schema as needed.
# The parquet data source automatically detect this case and 
# merges schemas of all these files.

# Schema merging is an expensive operation. You may enable it by: 
# 1. setting data source option mergeSchema to true when reading Parquet files (as shown in the examples below), or
# 2. setting the global SQL option spark.sql.parquet.mergeSchema to true.

from pyspark.sql import Row

# spark is from the previous example.
# Create a simple DataFrame, stored into a partition directory
sc = spark.sparkContext

squaresDF = spark.createDataFrame(sc.parallelize(range(1, 6))
                                  .map(lambda i: Row(single=i, double=i ** 2)))
squaresDF.write.parquet("data/test_table/key=1")

# Create another DataFrame in a new partition directory,
# adding a new column and dropping an existing column
cubesDF = spark.createDataFrame(sc.parallelize(range(6, 11))
                                .map(lambda i: Row(single=i, triple=i ** 3)))
cubesDF.write.parquet("data/test_table/key=2")

# Read the partitioned table
mergedDF = spark.read.option("mergeSchema", "true").parquet("data/test_table")
mergedDF.printSchema()

# The final schema consists of all 3 columns in the Parquet files together
# with the partitioning column appeared in the partition directory paths.


# In[11]:


spark.stop()
sc.stop()


# In[ ]:




