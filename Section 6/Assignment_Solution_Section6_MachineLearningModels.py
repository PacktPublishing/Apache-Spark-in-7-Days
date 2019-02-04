#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# The classification goal is to predict whether the client will 
# subscribe (Yes/No) to a bank term deposit.
# Dataset can be found on Kaggle (Bank Marketing) and UCI
# https://www.kaggle.com/rouseguy/bankbalanced/data


# In[1]:


# start the Spark Context
import findspark
findspark.init()


# In[2]:


import pyspark 
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

import pandas as pd
import matplotlib


# In[3]:


# Question 1. Read in the csv data file, bank.csv with header = True and
# inferSchema = True. Assign to variable df. Then, use printSchema() on df.
# Read in data file and print schema
df = spark.read.csv('bank.csv', header = True, inferSchema = True)
# print schema
df.printSchema()


# In[4]:


# Question 2. Use df.select('deposit') and use the count() to see how
# many elements (data points) are there.
# Count total number on outcome variable
df.select('deposit').count()


# In[5]:


# Question 3.  Use df.select() on 'deposit', then groupby() on 'deposit',and then count().
# Display the result with show(). You can chain all these operations in 1 line of code.
# Check to see if the outcome variable's binary classes are balanced
df.select('deposit').groupby('deposit').count().show()


# In[6]:


# Question 4. Take a look at the column names and data types.
# Use df.dtypes

# take a look at the datatypes on the variables
df.dtypes


# In[7]:


# Execute code below
# pull out numeric features from list of datatypes
numeric_features = [t[0] for t in df.dtypes if t[1] == 'int']

# Display pandas dataframe to show columns with numeric features
df.select(numeric_features).describe().toPandas().transpose()


# In[10]:


# Execute code in cell below. Wait for output to see visualization 
# before continuing on to next cell.
# Create correlation matrix amongst numeric variables

numeric_data = df.select(numeric_features).toPandas()
axs = pd.plotting.scatter_matrix(numeric_data, figsize=(8, 8));
n = len(numeric_data.columns)
for i in range(n):
    v = axs[i, 0]
    v.yaxis.label.set_rotation(0)
    v.yaxis.label.set_ha('right')
    v.set_yticks(())
    h = axs[n-1, i]
    h.xaxis.label.set_rotation(90)
    h.set_xticks(())


# In[11]:


# Question 5.  Execute df.columns.
# Take a look at columns
df.columns


# In[12]:


# Question 6.  Create a new dataframe, df, and select from current dataframe df
# 'age', 'job', 'marital', 'education', 'default', 'balance', 'housing', 
# 'loan', 'contact', 'duration','campaign', 'pdays', 'previous', 
# 'poutcome', and 'deposit'. 
# Set new variable name, cols equal to df.columns.
# Print schema.

# Create a new dataframe, specifying all variables except 'day' and 'month'.
df = df.select('age', 'job', 'marital', 'education', 'default', 
               'balance', 'housing', 'loan', 'contact', 'duration', 
               'campaign', 'pdays', 'previous', 'poutcome', 'deposit')

# Set new variable name, cols equal to df.columns.
cols = df.columns
# print schema
df.printSchema()


# Preprocessing the data with indexing, encoding, and assembling

# In[13]:


# Question 7. Fill in empty slots (empty spaces).
# Preprocessing the data for category indexing, one-hot encoding, and 
# vector assembling.
# Code is from Databricks

from pyspark.ml.feature import OneHotEncoderEstimator, StringIndexer, VectorAssembler

categoricalColumns = ['job', 'marital', 'education', 'default', 'housing', 'loan', 'contact', 'poutcome']
stages = []

for categoricalCol in categoricalColumns:
    
    stringIndexer = StringIndexer(inputCol = categoricalCol, outputCol = categoricalCol + 'Index')
    encoder = OneHotEncoderEstimator(inputCols=[stringIndexer.getOutputCol()], outputCols=[categoricalCol + "classVec"])
    stages += [stringIndexer, encoder]
    
    
label_stringIdx = StringIndexer(inputCol = 'deposit', outputCol = 'label')
stages += [label_stringIdx]


numericCols = ['age', 'balance', 'duration', 'campaign', 'pdays', 'previous']
assemblerInputs = [c + "classVec" for c in categoricalColumns] + numericCols


assembler = VectorAssembler(inputCols=assemblerInputs, outputCol="features")
stages += [assembler]


# Create Machine Learning Pipeline

# In[14]:


# Question 8. Fill in empty slots (empty spaces)
from pyspark.ml import Pipeline

pipeline = Pipeline(stages = stages)

pipelineModel = pipeline.fit(df)

df = pipelineModel.transform(df)

selectedCols = ['label', 'features'] + cols

df = df.select(selectedCols)

df.printSchema()


# In[15]:


# Execute code in cell below.

pd.DataFrame(df.take(5), columns=df.columns).transpose()


# Split data into train and test dataset 

# In[16]:


# Question 9.  Use randomSplit() on df and split, train, test datasets in
# 70/30 splits. Set the seed to 777. Save in variables train, test.
train, test = df.randomSplit([0.7, 0.3], seed = 777)


print("Training Dataset Count: " + str(train.count()))
print("Test Dataset Count: " + str(test.count()))


# Logistic Regression Model

# In[17]:


# Question 10. Use LogisticRegression() and set featuresCol to 'features',
# labelCol to 'label', and maxIter to 10. Save in variable named lr.
# Use fit() on lr, passing in the train dataset. Save in variable named lrModel.

from pyspark.ml.classification import LogisticRegression

lr = LogisticRegression(featuresCol = 'features', labelCol = 'label', maxIter=10)
lrModel = lr.fit(train)


# In[18]:


# Execute code in cell
import matplotlib.pyplot as plt

trainingSummary = lrModel.summary
roc = trainingSummary.roc.toPandas()
plt.plot(roc['FPR'],roc['TPR'])
plt.ylabel('False Positive Rate')
plt.xlabel('True Positive Rate')
plt.title('ROC Curve')
plt.show()
print('Training set areaUnderROC: ' + str(trainingSummary.areaUnderROC))


# In[19]:


# Execute code in cell
pr = trainingSummary.pr.toPandas()
plt.plot(pr['recall'],pr['precision'])
plt.ylabel('Precision')
plt.xlabel('Recall')
plt.show()


# In[20]:


# Question 11. use transform() on lrModel by inputting the test dataset. 
# Save in variable named predictions.
# Select 'age', 'job', 'label', 'rawPrediction', 'prediction', 'probability' from predictions.
# Display output with show(10).

predictions = lrModel.transform(test)
predictions.select('age', 'job', 'label', 'rawPrediction', 'prediction', 'probability').show(10)


# Logistic Regression model evaluation

# In[21]:


# Question 12.  Set evaluator = BinaryClassificationEvaluator()

from pyspark.ml.evaluation import BinaryClassificationEvaluator

evaluator = BinaryClassificationEvaluator()

print('Test Area Under ROC', evaluator.evaluate(predictions))


# Gradient-Boosted Tree Classifier

# In[22]:


# Question 13.  Fill in empty slots (empty spaces)

from pyspark.ml.classification import GBTClassifier

gbt = GBTClassifier(maxIter=10)

gbtModel = gbt.fit(train)

predictions = gbtModel.transform(test)
predictions.select('age', 'job', 'label', 'rawPrediction', 'prediction', 'probability').show(5)


# Gradient-Boosted Tree Classifier evaluation

# In[23]:


# Question 14.  Set evaluator equal to BinaryClassificationEvaluator()

evaluator = BinaryClassificationEvaluator()

print("Test Area Under ROC: " + str(evaluator.evaluate(predictions, {evaluator.metricName: "areaUnderROC"})))


# Cross-Validation

# In[24]:


# Question 15. Fill in empty slots (empty spaces).
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator

paramGrid = (ParamGridBuilder()
             .addGrid(gbt.maxDepth, [2, 4, 6])
             .addGrid(gbt.maxBins, [20, 60])
             .addGrid(gbt.maxIter, [10, 20])
             .build())


cv = CrossValidator(estimator=gbt, estimatorParamMaps=paramGrid, evaluator=evaluator, numFolds=5)

# Run cross validations.  This can take about 10 minutes since it is training over 20 trees!
cvModel = cv.fit(train)

predictions = cvModel.transform(test)

evaluator.evaluate(predictions)


# In[ ]:


spark.stop()


# In[ ]:




