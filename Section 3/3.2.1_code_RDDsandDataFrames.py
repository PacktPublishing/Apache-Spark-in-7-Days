


# start the Spark Context
import findspark
findspark.init()





# import SparkContext    
from pyspark import SparkContext
sc = SparkContext(master="local[3]")  # run locally on your computer, 1 worker per core, there are 3 cores
# recommendation is to run 1 worker per core
print(sc)





# create rdd
rdd1 = sc.parallelize(range(10))
rdd1





# count the number of elements
rdd1.count()





# collect returns a list
rdd1.collect()





# example of map
rdd1.map(lambda x: x*x).collect()





# reduce operation that finds the shortest string in an RDD
words = ['Spark', 'is', 'really', 'awesome', 'and', 'fun']
wordRDD=sc.parallelize(words)
wordRDD.reduce(lambda w,v: w if len(w) < len(v) else v)





# Before starting a new SparkContext, be sure to stop the current one
# Stop the SparkContext kernel
sc.stop()







