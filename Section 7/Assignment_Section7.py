#!/usr/bin/env python
# coding: utf-8

# MLLib and Continuous Application with Batch and Simulated Streaming Data 

# A copy of this data and its licence are available at https://s3-us-west-2.amazonaws.com/ml-team-public-read/credit-card-fraud.zip
# 
# Source:  https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/8599738367597028/68280419113053/3601578643761083/latest.html
# 

# In[ ]:


# Execute this cell
import findspark
findspark.init()


# In[ ]:


# Execute this cell
import pyspark 
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()


# In[ ]:


# Question 1.  Read in 'credit-card-fraud/data' as parquet format and save
# in variable named data.  Print the schema of data.


# In[ ]:


# Question 2. Use the count() function on data.


# In[ ]:


# Question 3. Display the data with show().


# In[ ]:


# Execute this cell
from pyspark.ml.feature import OneHotEncoderEstimator, VectorAssembler, VectorSizeHint
from pyspark.ml.classification import GBTClassifier

from pyspark.sql.types import *
from pyspark.sql.functions import count, rand, collect_list, explode, struct, count


# In[ ]:


# Question 4. Use OneHotEncoderEstimator() with inputCols=["amountRange"], outputCols=["amountVect"].
# Save in variable named oneHot.


# In[ ]:


# Question 5.  Use VectorAssember() with inputCols=["amountVect", "pcaVector"], outputCol="features".
# Save in variable named vectorAsember.


# In[ ]:


# Question 6.  Use GBTClassifier() with labelCol="label", featuresCol="features".
# Save in variable named estimator


# In[ ]:


# Execute this cell
# When using MLlib with structured streaming, VectorAssembler has 
# some limitations in a streaming context. Specifically, VectorAssembler 
# can only work on Vector columns of known size. To address this issue we 
# can explicitly specify the size of the pcaVector column so that we'll 
# be be able to use our pipeline with structured streaming. To do this 
# we'll use the VectorSizeHint transformer.

from pyspark.ml.feature import VectorSizeHint


# In[ ]:


# Question 7. Use VectorSizeHint() with inputCol="pcaVector", size=28.


# In[ ]:


# Execute this cell
from pyspark.ml import Pipeline
from pyspark.sql.functions import col


# In[ ]:


# Question 8. Create a Pipeline() and include the stages equal to a 
# list of oneHot, vectorSizeHint, vectorAssembler, estimator. Save in
# a variable named pipeline.


# In[ ]:


# Execute this cell
# let's split the data into testing and training datasets. 
# We will shave the test dataset for later
#
#
train = data.filter(col("time") % 10 < 8)
test = data.filter(col("time") % 10 >= 8)
#
# save our data into partitions so we can read them as files
#
(test.repartition(20).write
  .mode("overwrite")
  .parquet("test-data"))


# In[ ]:


# Question 9. Use the count function on the train dataset.


# In[ ]:


# Question 10.  Use the count function on the test dataset.


# In[ ]:


# Question 11. Fit the train dataset on the pipeline and save
# in a variable named pipelineModel.


# In[ ]:


# Execute this cell.
# Simulate a stream by reading from a test data file. Typically, you would
# use a Kafka cluster and read off Kafka topics.
from pyspark.sql.types import *
from pyspark.ml.linalg import VectorUDT


# In[ ]:


# Question 12. Create a schema for the batch data.
# Use StructType with four StructFields. Specify the input for each as:
# "time", IntegerType(), True
# "amountRange", IntegerType(), True
# "label", IntegerType(), True
# "pcaVector", VectorUDT(), True
# Save in variable name schema.



# In[ ]:


# Question 13. Use spark.read and specify the schema, option with 
# maxFilesPerTrigger with 1 second, and parquet for "test-data".
# Save in variable named streamingData.

               


# In[ ]:


# Execute this cell
from pyspark.sql.functions import *


# In[ ]:


# Question 14.  Use transform() on pipelineModel, passing in streamingData.
# Save in variable named stream.


# In[ ]:


# Question 15.  Use pipelineModel.transform() passing in streaming Data.
# Use groupBy() with inputs "label", "prediction".
# Use count() with no inputs.
# Use sort() with inputs "label", "prediction".
# Save in variable streamPredictions.



# In[ ]:


# Execute this cell
import pandas as pd


# In[ ]:


# Question 16.  Convert to pandas dataframe with streamPredictions.toPandas()  
# Display output


# In[ ]:


spark.stop()


# In[ ]:




