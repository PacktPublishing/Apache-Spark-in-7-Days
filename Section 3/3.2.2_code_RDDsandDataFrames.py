


# find spark
import findspark
findspark.init()





# source:  https://spark.apache.org/docs/2.3.0/sql-programming-guide.html#datasets-and-dataframes
# The entry point into all functionality in Spark is the SparkSession class. 
# To create a basic SparkSession, just use SparkSession.builder:
from pyspark.sql import SparkSession
spark = SparkSession     .builder     .appName("Python Spark SQL basic example")     .config("spark.some.config.option", "some-value")     .getOrCreate()





# read in a dataframe
df = spark.read.json("people.json")
df





#display dataframe
df.show()





# Print the schema in a tree format
df.printSchema()





# Select only the "name" column
df.select("name").show()





# Select everybody, but increment the age by 1
df.select(df['name'], df['age'] + 1).show()





# Select people older than 21
df.filter(df['age'] > 21).show()




spark.stop()

















