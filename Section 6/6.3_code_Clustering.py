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


# K-Means

# In[3]:


# k-means is one of the most commonly used clustering algorithms that clusters 
# the data points into a predefined number of clusters.
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator


# In[4]:


# Loads data.
dataset = spark.read.format("libsvm").load("sample_kmeans_data.txt")


# In[5]:


dataset.show()


# In[6]:


# Trains a k-means model.
kmeans = KMeans().setK(2).setSeed(1)
model = kmeans.fit(dataset)


# In[7]:


# Make predictions
predictions = model.transform(dataset)


# In[8]:


predictions.show()


# In[9]:


# Evaluate clustering by computing Silhouette score
evaluator = ClusteringEvaluator()


# In[10]:


# The silhouette value is a measure of how similar an object is to its 
# own cluster (cohesion) compared to other clusters (separation).
silhouette = evaluator.evaluate(predictions)
print("Silhouette with squared euclidean distance = " + str(silhouette))


# In[11]:


# Shows the result.
centers = model.clusterCenters()
print("Cluster Centers: ")
for center in centers:
    print(center)


# Latent Dirichlet Allocation (LDA)

# In[12]:


# LDA represents documents as a mixture of topics that identify words with 
# certain probabilities.
from pyspark.ml.clustering import LDA


# In[13]:


# Loads data.
dataset = spark.read.format("libsvm").load("sample_lda_libsvm_data.txt")


# In[14]:


dataset.show()


# In[15]:


# Trains a LDA model.
lda = LDA(k=10, maxIter=10)
model = lda.fit(dataset)


# In[16]:


ll = model.logLikelihood(dataset)
lp = model.logPerplexity(dataset)
print("The lower bound on the log likelihood of the entire corpus: " + str(ll))
print("The upper bound on perplexity: " + str(lp))


# In[17]:


# Describe topics.
topics = model.describeTopics(3)
print("The topics described by their top-weighted terms:")
topics.show(truncate=False)


# In[18]:


# Shows the result of label, features, and topic Distribution
transformed = model.transform(dataset)
transformed.show(truncate=False)


# In[19]:


spark.stop()


# In[ ]:




