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


# Linear Regression

# In[3]:


from pyspark.ml.regression import LinearRegression


# In[4]:


# Load training data
training = spark.read.format("libsvm")    .load("sample_linear_regression_data.txt")


# In[5]:


lr = LinearRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8)


# In[6]:


# Fit the model
lrModel = lr.fit(training)


# In[7]:


# Print the coefficients and intercept for linear regression
print("Coefficients: %s" % str(lrModel.coefficients))
print("Intercept: %s" % str(lrModel.intercept))


# In[8]:


# Summarize the model over the training set and print out some metrics
trainingSummary = lrModel.summary
print("numIterations: %d" % trainingSummary.totalIterations)
print("objectiveHistory: %s" % str(trainingSummary.objectiveHistory))


# In[9]:


trainingSummary.residuals.show()
print("RMSE: %f" % trainingSummary.rootMeanSquaredError)
print("r2: %f" % trainingSummary.r2)


# In[ ]:


# end of section on Linear Regression


# Gradient-Boosted Tree Regression

# In[10]:


from pyspark.ml import Pipeline
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.feature import VectorIndexer
from pyspark.ml.evaluation import RegressionEvaluator


# In[11]:


# Load and parse the data file, converting it to a DataFrame.
data = spark.read.format("libsvm").load("sample_libsvm_data.txt")


# In[12]:


# Automatically identify categorical features, and index them.
# Set maxCategories so features with > 4 distinct values are treated as continuous.
featureIndexer =    VectorIndexer(inputCol="features", outputCol="indexedFeatures", maxCategories=4).fit(data)


# In[13]:


# Split the data into training and test sets (30% held out for testing)
(trainingData, testData) = data.randomSplit([0.7, 0.3])


# In[14]:


# Train a GBT model.
gbt = GBTRegressor(featuresCol="indexedFeatures", maxIter=10)


# In[15]:


# Chain indexer and GBT in a Pipeline
pipeline = Pipeline(stages=[featureIndexer, gbt])


# In[16]:


# Train model.  This also runs the indexer.
model = pipeline.fit(trainingData)


# In[17]:


# Make predictions.
predictions = model.transform(testData)


# In[18]:


# Select example rows to display.
predictions.select("prediction", "label", "features").show(5)


# In[19]:


# Select (prediction, true label) and compute test error
evaluator = RegressionEvaluator(
    labelCol="label", predictionCol="prediction", metricName="rmse")


# In[20]:


rmse = evaluator.evaluate(predictions)
print("Root Mean Squared Error (RMSE) on test data = %g" % rmse)


# In[21]:


gbtModel = model.stages[1]
print(gbtModel)  # summary only


# In[22]:


spark.stop()


# In[ ]:




