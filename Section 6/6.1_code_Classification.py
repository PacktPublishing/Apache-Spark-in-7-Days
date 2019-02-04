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


# Binomial Logistic Regression

# In[3]:


# logistic model with binary dependent variable
from pyspark.ml.classification import LogisticRegression


# In[4]:


# Load training data
training = spark.read.format("libsvm").load("sample_libsvm_data.txt")


# In[5]:


lr = LogisticRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8)


# In[6]:


# Fit the model
lrModel = lr.fit(training)


# In[7]:


# Print the coefficients and intercept for logistic regression
print("Coefficients: " + str(lrModel.coefficients))
print("Intercept: " + str(lrModel.intercept))


# In[8]:


# We can also use the multinomial family for binary classification
mlr = LogisticRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8, family="multinomial")


# In[9]:


# Fit the model
mlrModel = mlr.fit(training)


# In[10]:


# Print the coefficients and intercepts for logistic regression with multinomial family
print("Multinomial coefficients: " + str(mlrModel.coefficientMatrix))
print("Multinomial intercepts: " + str(mlrModel.interceptVector))


# In[ ]:


# end of section on Binomial Logistic Regression


# In[ ]:


Naive Bayes


# In[11]:


# Classification model based on Bayes' Theorem with an assumption of independence
# among predictors.  A NB classifier assumes that the presence of a particular
# feature in a class is unrelated to the presence of any other feature.
from pyspark.ml.classification import NaiveBayes
from pyspark.ml.evaluation import MulticlassClassificationEvaluator


# In[12]:


# Load training data
data = spark.read.format("libsvm")     .load("sample_libsvm_data.txt")


# In[13]:


# Split the data into train and test
splits = data.randomSplit([0.6, 0.4], 1234)
train = splits[0]
test = splits[1]


# In[14]:


train.show()


# In[15]:


test.show()


# In[16]:


# create the trainer and set its parameters
nb = NaiveBayes(smoothing=1.0, modelType="multinomial")


# In[17]:


# train the model
model = nb.fit(train)


# In[18]:


# select example rows to display.
predictions = model.transform(test)
predictions.show()


# In[19]:


# compute accuracy on the test set
evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction",
                                              metricName="accuracy")


# In[20]:


accuracy = evaluator.evaluate(predictions)
print("Test set accuracy = " + str(accuracy))


# In[22]:


spark.stop()


# In[ ]:




