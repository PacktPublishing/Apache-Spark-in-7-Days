

# To submit an application to Spark, provide filepath after bin spark-submit
bin/spark-submit python/filename.py

# To submit locally (not on the worker nodes):
bin/spark-submit python/filename.py --master local[8] --executor-memory 20G
# For the above, it instructs your application to be deployed in local mode with a
# a total of 8 executor cores.

# To submit an application to Spark on the worker nodes (cluster), you
# would supply the URL of the spark master node
bin/spark-submit python/filename.py --master spark://12.345.45.678:7077