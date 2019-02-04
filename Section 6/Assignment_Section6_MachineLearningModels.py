#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# The classification goal is to predict whether the client will 
# subscribe (Yes/No) to a bank term deposit.
# Dataset can be found on Kaggle (Bank Marketing) and UCI
# https://www.kaggle.com/rouseguy/bankbalanced/data


# In[ ]:


# start the Spark Context
import findspark
findspark.init()


# In[ ]:


import pyspark 
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

import pandas as pd
import matplotlib


# In[ ]:


# Question 1. Read in the csv data file, bank.csv with header = True and
# inferSchema = True. Assign to variable df. Then, use printSchema() on df.

# Read in data file 

# print schema


# In[ ]:


# Question 2. Use df.select('deposit') and use the count() to see how
# many elements (data points) are there.
# Count total number on outcome variable



# In[ ]:


# Question 3.  Use df.select() on 'deposit', then groupby() on 'deposit',and then count().
# Display the result with show(). You can chain all these operations in 1 line of code.
# Check to see if the outcome variable's binary classes are balanced



# In[ ]:


# Question 4. Take a look at the column names and data types.
# Use df.dtypes

# take a look at the datatypes on the variables


# In[ ]:


# Execute code in cell  
# pull out numeric features from list of datatypes
numeric_features = [t[0] for t in df.dtypes if t[1] == 'int']

# Display pandas dataframe to show columns with numeric features
df.select(numeric_features).describe().toPandas().transpose()


# In[ ]:


# Execute code in cell below. Wait for output to see visualization 
# before continuing on to next cell.
# Create correlation matrix amongst numeric variables

numeric_data = df.select(numeric_features).toPandas()
# Use pandas.plotting.scatter_matrix() to check correlations amongst variables
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


# In[ ]:


# Question 5.  Execute df.columns.
# Take a look at columns


# In[ ]:


# Question 6.  Create a new dataframe, df, and select from current dataframe df
# 'age', 'job', 'marital', 'education', 'default', 'balance', 'housing', 
# 'loan', 'contact', 'duration','campaign', 'pdays', 'previous', 
# 'poutcome', and 'deposit'. 
# Set new variable name, cols equal to df.columns.
# Print schema.

# Create a new dataframe, specifying all variables except 'day' and 'month'.




# Set new variable name, cols equal to df.columns.

# Print schema.


# Preprocessing the data with indexing, encoding, and assembling

# In[ ]:


# Question 7. Fill in empty slots (empty spaces).
# Preprocessing the data for category indexing, one-hot encoding, and 
# vector assembling.Code is from Databricks

from pyspark.ml.feature import OneHotEncoderEstimator, StringIndexer, VectorAssembler

categoricalColumns = ['job', 'marital', 'education', 'default', 'housing', 'loan', 'contact', 'poutcome']
stages = []

for categoricalCol in categoricalColumns:
    
    stringIndexer = StringIndexer(inputCol =          , outputCol =         + 'Index')
    encoder = OneHotEncoderEstimator(inputCols=[           .getOutputCol()], outputCols=[        + "classVec"])
    stages += [stringIndexer,        ]
    
    
label_stringIdx = StringIndexer(inputCol = 'deposit', outputCol =     )
stages += [label_stringIdx]


numericCols = [     ,      ,      ,       ,       ,      ]
assemblerInputs = [c + "classVec" for c in categoricalColumns] + numericCols


assembler = VectorAssembler(inputCols=        , outputCol="features")
stages += [        ]


# Create Machine Learning Pipeline

# In[ ]:


# Question 8. Fill in empty slots (empty spaces)
from pyspark.ml import Pipeline

pipeline = Pipeline(stages =     )

pipelineModel = pipeline.fit(  )

df = pipelineModel.      (df)

selectedCols = ['label',       ] + cols

df = df.select(       )

df.printSchema()


# In[ ]:


# Execute code in cell.
pd.DataFrame(df.take(5), columns=df.columns).transpose()


# Split data into train and test dataset 

# In[ ]:


# Question 9.  Use randomSplit() on df and split, train, test datasets in
# 70/30 splits. Set the seed to 777. Save in variables train, test.



print("Training Dataset Count: " + str(train.count()))
print("Test Dataset Count: " + str(test.count()))


# Logistic Regression Model

# In[ ]:


# Question 10. Use LogisticRegression() and set featuresCol to 'features',
# labelCol to 'label', and maxIter to 10. Save in variable named lr.
# Use fit() on lr, passing in the train dataset. Save in variable named lrModel.

from pyspark.ml.classification import LogisticRegression



# In[ ]:


# Execute code in cell below
import matplotlib.pyplot as plt

trainingSummary = lrModel.summary
roc = trainingSummary.roc.toPandas()
plt.plot(roc['FPR'],roc['TPR'])
plt.ylabel('False Positive Rate')
plt.xlabel('True Positive Rate')
plt.title('ROC Curve')
plt.show()
print('Training set areaUnderROC: ' + str(trainingSummary.areaUnderROC))


# In[ ]:


# Execute code in cell below
pr = trainingSummary.pr.toPandas()
plt.plot(pr['recall'],pr['precision'])
plt.ylabel('Precision')
plt.xlabel('Recall')
plt.show()


# In[ ]:


# Question 11. use transform() on lrModel by inputting the test dataset. 
# Save in variable named predictions.
# Select 'age', 'job', 'label', 'rawPrediction', 'prediction', 'probability' from predictions.
# Display output with show(10).


# Logistic Regression model evaluation

# In[ ]:


# Question 12.  Set evaluator equal to BinaryClassificationEvaluator().
from pyspark.ml.evaluation import BinaryClassificationEvaluator




print('Test Area Under ROC', evaluator.evaluate(predictions))


# Gradient-Boosted Tree Classifier

# In[ ]:


# Question 13.  Fill in empty slots (empty spaces)

from pyspark.ml.classification import GBTClassifier

gbt = GBTClassifier(maxIter=10)

gbtModel = gbt.fit(   )

predictions = gbtModel.      (test)

predictions.select('age', 'job', '     ', '      ', '       ', 'probability').show(5)


# Gradient-Boosted Tree Classifier evaluation

# In[ ]:


# Question 14.  Set evaluator equal to BinaryClassificationEvaluator()



print("Test Area Under ROC: " + str(evaluator.evaluate(predictions, {evaluator.metricName: "areaUnderROC"})))


# Cross-Validation

# In[ ]:


# Question 15. Fill in empty slots (empty spaces).
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator

paramGrid = (ParamGridBuilder()
             .addGrid(gbt.maxDepth, [2, 4, 6])
             .addGrid(gbt.maxBins, [20, 60])
             .addGrid(gbt.maxIter, [10, 20])
             .build())


cv = CrossValidator(estimator=   , estimatorParamMaps=     ,
                     evaluator=    , numFolds=5)

# Run cross validations.  This can take about 10 minutes since it is training over 20 trees!
cvModel = cv.fit(    )

predictions = cvModel.     (test)

evaluator.evaluate(        )


# In[ ]:


spark.stop()


# In[ ]:




