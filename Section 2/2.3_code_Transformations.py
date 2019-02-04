# example
import random
data = [7]*5 + [random.randint(0,100) for i in xrange(95)]
print(data)

random.shuffle(data)
print(data)

# Entry point to Spark is spark context
datardd = sc.parallelize(data)

# filter data based on a condition
data_sifted = datardd.filter(lambda x: x < 85)
data_sifted.collect()

# count the number of elements in the dataset
data_sifted.count()

# map a function
data_altered = data_sifted.map(lambda x: (x - 25) * 2/3)
data_altered.collect()

# find the mean
data_altered.mean()

# chaining of operations
datardd.filter(lambda x: x < 85).map(lambda x: (x-25) * 2/3).mean()


# create an rdd by loading in an external file
lines = sc.textFile('kindness.txt')

# word count of text
# flatMap, map, and reduceByKey are all transformations
counts = lines.flatMap(lambda line: line.split(" ")) \
			  .map(lambda word: (word, 1)) \
			  .reduceByKey(lambda a, b: a + b)
# cache() allows you to increase application performance
# caching an rdd persists the data in memory
counts.cache()

# return all the elements in the collection, triggers computation
counts.collect()

# no computation required
counts.take(3)

# no computation required
counts.count()		  

# display the top five most used words
# sortByKey() is a transformation
topwords = counts.map(lambda (w, c): (c, w)) \
				 .sortByKey(ascending=False)

topwords.take(5)

# sort data based on a key, using key-value pairs
# sortByKey() is a transformation
test = [('mouse', 5), ('cat', 42), ('bat', 33), ('rat', 79), ('dog', 58), ('owl', 23)]
sc.parallelize(test).sortByKey(True, 1).collect()


# flatmap example
# flatmap() is a transformation
x = sc.parallelize(["spark is awesome", "life is good"], 2)
y = x.map(lambda x: x.split(' '))
# returns a nested list
print(y.collect())

# gives a flattened list with flatMap(), which is a transformation
z = x.flatMap(lambda x: x.split(' '))
# returns a list
print(z.collect())




