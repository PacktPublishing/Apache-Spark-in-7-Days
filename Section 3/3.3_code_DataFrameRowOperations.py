#!/usr/bin/env python
# coding: utf-8

# In[1]:


# In the first cell, type:
import findspark
findspark.init()


# In[2]:


# In the second cell, type:
import pyspark # only run after findspark.init()
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()


# In[3]:


# Create a dataframe, use toDF()
df = spark.range(500).toDF("number")
df.show()


# In[4]:


# alter values  in rows
df.select(df["number"] + 10).show()


# In[5]:


# Filter row such that number module 2 is not equal to 0.
df.filter(df['number'] % 2 != 0).show()


# In[6]:


# use rdd to convert to rdd and perform rdd operations
df.rdd.count()


# In[7]:


# take the first 10 elements
df.rdd.take(10)


# In[8]:


# create a row object
spark.range(10).collect()


# In[9]:


# creating a row
from pyspark.sql import Row
myRow = Row("Hello", None, 1, False)
type(myRow)


# In[10]:


# return the first element
myRow[0]


# In[11]:


# return the third element
myRow[2]


# In[12]:


from pyspark.sql import Row 

from pyspark.sql.types import StructField, StructType, StringType, LongType

myManualSchema =StructType([
	StructField("some", StringType(), True),
	StructField("col", StringType(), True),
	StructField("names", LongType(), False)
])


# In[13]:


# Create a row
myRow = Row("Hello", None, 1)


# In[14]:


# Create dataframe from a row
myDf = spark.createDataFrame([myRow], myManualSchema)


# In[15]:


myDf.show()


# In[16]:


# Example of using Row function to create a dataframe
from pyspark.sql import Row
cats = Row("Name", "Nickname", "Location", "Treat")


# In[17]:


cat1 = Row('Dakota', 'Sweetie', "house", "salmon")
cat2 = Row('George', 'Grumpy', "apt", "liver")
cat3 = Row('Karrot', 'BiggieK', "condo", "chicken")
cat4 = Row('Tigress', 'Claw', "street", "trout")
cat5 = Row('Kitty', 'Meow', "house", "salmon")


# In[18]:


print(cat3)


# In[19]:


# Create Row elements
shelter1 = Row(id='23456', name='CatColony')
shelter2 = Row(id='11111', name='Mauhaus')
shelter3 = Row(id='98765', name='BigCatHouse')
shelter4 = Row(id='56789', name='WindowCats')


# In[20]:


print(shelter2)


# In[21]:


# Create Row elements
shelterWithCats1 = Row(shelter=shelter1, cats=[cat1, cat2])
shelterWithCats2 = Row(shelter=shelter2, cats=[cat3, cat4])
shelterWithCats3 = Row(shelter=shelter3, cats=[cat5, cat4, cat1])
shelterWithCats4 = Row(shelter=shelter4, cats=[cat2, cat3])


# In[22]:


shelterWithCats = [shelterWithCats1, shelterWithCats2, shelterWithCats3, shelterWithCats4]


# In[23]:


# Create dataframe
dframe = spark.createDataFrame(shelterWithCats)


# In[24]:


# Show dataframe
dframe.show()


# In[25]:


# Stop the Spark Context
spark.stop()


# In[ ]:




