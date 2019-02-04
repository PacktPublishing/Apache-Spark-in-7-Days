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


# Collaborative Filtering

# In[3]:


# Collaborative Filtering is a mean of recommendation 
# based on users’ past behavior.
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.sql import Row


# In[4]:


lines = spark.read.text("sample_movielens_ratings.txt").rdd
parts = lines.map(lambda row: row.value.split("::"))
ratingsRDD = parts.map(lambda p: Row(userId=int(p[0]), movieId=int(p[1]),
                                     rating=float(p[2])))


# In[5]:


ratings = spark.createDataFrame(ratingsRDD)


# In[6]:


ratings.show()


# In[7]:


(training, test) = ratings.randomSplit([0.8, 0.2])


# In[8]:


# Build the recommendation model using ALS on the training data
# Note we set cold start strategy to 'drop' to ensure we don't get NaN evaluation metrics
als = ALS(maxIter=5, regParam=0.01, userCol="userId", itemCol="movieId", ratingCol="rating",
          coldStartStrategy="drop")


# In[9]:


model = als.fit(training)


# In[10]:


# Evaluate the model by computing the RMSE on the test data
predictions = model.transform(test)
evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating",
                                predictionCol="prediction")


# In[11]:


rmse = evaluator.evaluate(predictions)
print("Root-mean-square error = " + str(rmse))


# In[12]:


# Generate top 10 movie recommendations for each user
userRecs = model.recommendForAllUsers(10)


# In[13]:


userRecs.show()


# In[14]:


# Generate top 10 user recommendations for each movie
movieRecs = model.recommendForAllItems(10)


# In[15]:


movieRecs.show()


# In[16]:


# Generate top 10 movie recommendations for a specified set of users
users = ratings.select(als.getUserCol()).distinct().limit(3)
userSubsetRecs = model.recommendForUserSubset(users, 10)


# In[17]:


userSubsetRecs.show()


# In[18]:


# Generate top 10 user recommendations for a specified set of movies
movies = ratings.select(als.getItemCol()).distinct().limit(3)
movieSubSetRecs = model.recommendForItemSubset(movies, 10)


# In[19]:


movieSubSetRecs.show()


# In[20]:


spark.stop()


# In[ ]:




