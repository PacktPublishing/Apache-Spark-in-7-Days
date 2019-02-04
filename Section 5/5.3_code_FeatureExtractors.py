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


# Term Frequency-Inverse Document Frequence (TF-IDF)

# In[3]:


from pyspark.ml.feature import HashingTF, IDF, Tokenizer


# In[4]:


sentenceData = spark.createDataFrame([
    (0.0, "Hi I heard about Spark"),
    (0.0, "I wish Java could use case classes"),
    (1.0, "Logistic regression models are neat")
], ["label", "sentence"])


# In[5]:


sentenceData.show()


# In[6]:


tokenizer = Tokenizer(inputCol="sentence", outputCol="words")
wordsData = tokenizer.transform(sentenceData)


# In[7]:


wordsData.show()


# In[8]:


hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures", numFeatures=20)
featurizedData = hashingTF.transform(wordsData)
# alternatively, CountVectorizer can also be used to get term frequency vectors


# In[9]:


featurizedData.show()


# In[10]:


# Inverse Document Frequency
idf = IDF(inputCol="rawFeatures", outputCol="features")


# In[11]:


idf


# In[12]:


# IDF model
idfModel = idf.fit(featurizedData)


# In[13]:


idfModel


# In[14]:


rescaledData = idfModel.transform(featurizedData)


# In[15]:


rescaledData


# In[16]:


rescaledData.select("label", "features").show()


# In[ ]:


# End of section on Term Frequency-Inverse Document Frequence (TF-IDF)


# Word2Vec
# 

# In[17]:


from pyspark.ml.feature import Word2Vec


# In[18]:


# Input data: Each row is a bag of words from a sentence or document.
documentDF = spark.createDataFrame([
    ("Hi I heard about Spark".split(" "), ),
    ("I wish Java could use case classes".split(" "), ),
    ("Logistic regression models are neat".split(" "), )
], ["text"])


# In[19]:


documentDF.show()


# In[20]:


# Learn a mapping from words to Vectors.
word2Vec = Word2Vec(vectorSize=3, minCount=0, inputCol="text", outputCol="result")


# In[21]:


word2Vec


# In[22]:


# build model
model = word2Vec.fit(documentDF)


# In[23]:


result = model.transform(documentDF)
for row in result.collect():
    text, vector = row
    print("Text: [%s] => \nVector: %s\n" % (", ".join(text), str(vector)))


# In[ ]:


# end of section on Word2Vec


# CountVectorizer

# In[24]:


from pyspark.ml.feature import CountVectorizer


# In[25]:


# Input data: Each row is a bag of words with a ID.
df = spark.createDataFrame([
    (0, "a b c".split(" ")),
    (1, "a b b c a".split(" "))
], ["id", "words"])


# In[26]:


df.show()


# In[27]:


# fit a CountVectorizerModel from the corpus.
cv = CountVectorizer(inputCol="words", outputCol="features", vocabSize=3, minDF=2.0)


# In[28]:


cv


# In[29]:


model = cv.fit(df)


# In[30]:


model


# In[31]:


result = model.transform(df)
result.show(truncate=False)


# In[32]:


spark.stop()


# In[ ]:




