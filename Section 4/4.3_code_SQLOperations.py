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


import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


# In[4]:


sc = spark.sparkContext


# In[5]:


# dataset as a list
data = [('patty', 'spring', 'baseball', 64),
        ('patty', 'autumn', 'soccer', 78),
        ('matty', 'autumn', 'hockey', 90),
        ('matty', 'spring', 'soccer', 64),
        ('cathy', 'spring', 'baseball', 100),
        ('cathy', 'autumn', 'hockey', 78),
        ('sandy', 'autumn', 'soccer', 50),
        ('joey', 'summer', 'soccer', 73),
        ('tammy', 'spring', 'soccer', 86),
        ('marley', 'autumn', 'hockey', 100)]


# In[6]:


# Create an rdd
rdd = sc.parallelize(data)


# In[7]:


rdd


# In[8]:


# create a dataframe from an rdd and name the columns
df = spark.createDataFrame(rdd, ['player', 'season', 'sport', 'ranking'])


# In[9]:


# display dataframe
df.show()


# In[11]:


# create another dataset called meta
meta = [('patty', 'community', 25),
        ('matty', 'college', 35),
        ('cathy', 'community', 40),
        ('sandy', 'college', 60),
        ('joey', 'community', 55),
        ('tammy', 'college', 23),
        ('marley', 'community', 45)]


# In[12]:


# create schema
schema = StructType([
    StructField('player', StringType(), True),
    StructField('league', StringType(), True),
    StructField('age', IntegerType(), True)
])


# In[13]:


# create dataframe, using dataset and schema
df_meta = spark.createDataFrame(meta, schema)


# In[14]:


# show dataframe
df_meta.show()


# In[15]:


# right outer join
df_full = df.join(df_meta, on='player', how='rightouter')
df_full.show()


# In[16]:


# Create a temporary view table to use SQL
df_full.createOrReplaceTempView('table')


# In[17]:


# Select columns
spark.sql('select player, age from table').show()


# In[18]:


# Filter rows
spark.sql('select player, age from table where age > 25').distinct().show()


# In[19]:


# Mutate table
spark.sql('select player, age + 5 as adj_age from table').show()


# In[20]:


# Select columns
spark.sql('select player, age from table order by age desc').distinct().show()


# In[21]:


# Calculate mean
spark.sql('select mean(age) from table').show()


# In[22]:


# Split-apply-combine
q = '''
select league, mean(ranking), max(age)
from table
group by league
'''


# In[23]:


# Display result
spark.sql(q).show()


# In[25]:


# Use pyspark.sql.functions as built in functions

ranking_players = (
    F.
    when(F.col('ranking') > 90, 'Top Ten').
    when(F.col('ranking') > 80, 'Top Twenty').
    otherwise('average player')
)


# In[26]:


# Create another column in the dataframe
df.withColumn('player_standing', ranking_players).show()


# In[27]:


# Use write mode to overwrite file as a csv file
df_full.write.mode('overwrite').option('header', 'true').csv('listplayers.csv')


# In[28]:


# Use read option to read csv file
df1 = spark.read.option('header', 'true').csv('listplayers.csv')


# In[29]:


# Display results
df1.show()


# In[30]:


# Use write mode to overwrite as json file
df_full.write.mode('overwrite').json('listplayers.json')


# In[31]:


# Use read json to read file
df2 = spark.read.json('listplayers.json')


# In[32]:


# Display results
df2.show()


# In[33]:


# Use write mode to overwrite as parquet file
df_full.write.mode('overwrite').parquet('listplayers.parquet')


# In[34]:


# Use read parquet to read file
df3 = spark.read.parquet('listplayers.parquet')


# In[35]:


# Display result
df3.show()


# In[36]:


spark.stop()
sc.stop()


# In[ ]:




