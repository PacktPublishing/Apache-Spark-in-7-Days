# Instructions to download and install python 3.7 from Anaconda distribution
# with spark


# Install the python 3.7 version using the Anaconda distribution, go to:
# 	 https://www.anaconda.com/download/#macros

# To install a bunch of useful extensions, go to:
#     https://github.com/Jupyter-contrib/jupyter_nbextensions_configurator


# From instructions, in terminal, enter command:
conda install -c conda-forge jupyter_nbextensions_configurator

conda install -c conda-forge findspark=1.0.0

# Go to a directory in terminal (pick your own directory where you will work) for example:
# cd /Users/yourname/Desktop/foldername
cd /Users/karenyang/Desktop/spark_course

# In terminal, type command:
jupyter notebook

# Notice that it will open up http://localhost:8888/tree

# In the upper right area, go to "new" and select python 3
# In the first cell, type:
import findspark
findspark.init()
# click inside of cell and then use shift + enter(return) to execute cell.

# In the second cell, type:
import pyspark # only run after findspark.init()
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
df = spark.sql("select 'spark' as hello")
# click inside of cell and then use shift + enter(return) to execute cell.

# In the third cell, type:
df.show()
# click inside of cell and then use shift + enter(return) to execute cell.


# The output should look like:
+-----+
|hello|
+-----+
|spark|
+-----+





