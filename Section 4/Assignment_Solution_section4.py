#!/usr/bin/env python
# coding: utf-8

# In[1]:


# In the first cell, type:
import findspark
findspark.init()


# In[2]:


import pyspark # only run after findspark.init()
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext


# In[3]:


# other imports
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

import pandas as pd


# In[4]:


# Sample dataset 1
# Use this sample dataset that represents a  mock tobacco use research study

tobacco1_data = {'ParticipantId': {0: 1, 1: 2, 2: 3, 3: 4, 4: 5, 5: 6},
         'Name': {0: 'Joe', 1: 'Virginia', 2: 'Lucky', 3: 'Winston', 4: 'Marl', 5: 'Newport'},
         'Sex': {0: 'male', 1: 'female', 2: 'female', 3: 'male', 4: 'male', 5: 'female'},
         'Vaping': {0: 0, 1: 1, 2: 1, 3: 1, 4: 0, 5: 0}}


# In[5]:


# Sample dataset 2
tobacco2_data = {'ParticipantId': {0: 1, 1: 2, 2: 3, 3: 4, 4: 5, 5: 6},
         'Age': {0: 25, 1: 38, 2: 47, 3: 54, 4: 35, 5: 90},
         'CigarettesPerMonth': {0: 65, 1: 224, 2: 90, 3: 301, 4: 180, 5: 240},
         'StudyWave': {0: 3, 1: 2, 2: 2, 3: 3, 4: 4, 5: 4}}


# In[6]:


# create pandas dataframes df1_pd and df2_pd as shown below with code.
df1_pd = pd.DataFrame(tobacco1_data, columns=tobacco1_data.keys())
df2_pd = pd.DataFrame(tobacco2_data, columns=tobacco2_data.keys())


# In[7]:


# Question 1.  Create 2 dataframes, df1 and df2, with spark.createDataFrame().
# Use df1_pd and df2_pd as inputs.
df1 = spark.createDataFrame(df1_pd)
df2 = spark.createDataFrame(df2_pd)


# In[8]:


# Question 2.  Display df1 with show().
# Display dataframe as df1.
df1.show()


# In[9]:


# Question 3.  Display df2 with show().
# Display dataframe, df2.
df2.show()


# In[10]:


# Question 4. Join df1 with df2 on ParticipantID and save as df_full.
# Join the two dataframes
df_full = df1.join(df2, ['ParticipantId'])


# In[11]:


# Question 5. Verify object dataframe, df_full has been created.
df_full


# In[12]:


# Question 6. Display the dataframe, df_full with show().
df_full.show()


# In[13]:


# Question 7.
# Use pyspark.sql.functions as built in functions. Create a variable called 
# SmokerType and use the when clause to set up conditionals. When
# CigarettesPerMonth is greater than 200, denote it as 'Heavy'. When
# CigarettesPerMonth is greater than 100, denote it as 'Medium'. Otherwise,
# denote as 'Light'.
SmokerType = (
    F.
    when(F.col('CigarettesPerMonth') > 200, 'Heavy').
    when(F.col('CigarettesPerMonth') > 100, 'Medium').
    otherwise('Light')
)


# In[14]:


# Question 8. Create another column in the dataframe, 
# using the withColumn('Smoker Type', SmokerType)
# Use show() to display result.
df_full.withColumn('Smoker Type', SmokerType).show()


# In[15]:


# Question 9. Register the DataFrame as a SQL temporary view.
# Give the name 'table'.
df_full.createOrReplaceTempView('table')


# In[16]:


# Question 10. Select columns Name, Sex, Age, Vaping, and StudyWave from table.
# Use show() to display result.
spark.sql('select Name, Sex, Age, Vaping, StudyWave from table').show()


# In[17]:


# Question 11. Select all columns.
# Use show() to display result.
spark.sql('select * from table').show()


# In[18]:


# Question 12. Select Name and Age from table and filter rows
# where Age is less than 90 and Vaping is equal to 1.
# Use show() to display result.
spark.sql('select Name, Age from table           where Age < 90 and Vaping == 1').show()


# In[19]:


# Question 13. Mutate table by creating a new column named "CigarettesPerYear".
# Select from table Name, select Name, CigarettesPerMonth, and StudyWave, and 
# multiply CigarettesPerMonth by 12 as CigarettesPerYear. 
# Use show() to display result.
spark.sql('select Name, CigarettesPerMonth, StudyWave,           CigarettesPerMonth*12 as CigarettesPerYear from table').show()


# In[20]:


# Question 14. Select columns Name, Sex, Vaping, and CigarettesPerMonth.
# Order by CigarettesPerMonth in ascending order.
# Use show() to display result.
spark.sql('select Name, Sex, Vaping, CigarettesPerMonth from table           order by CigarettesPerMonth asc').show()


# In[21]:


# Question 15. Select max age from table.
# Use show() to display result.
spark.sql('select max(Age) from table').show()


# In[22]:


# stop the Spark session
spark.stop()
sc.stop()

