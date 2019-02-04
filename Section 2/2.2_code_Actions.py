

# create an rdd by loading in an external file
lines = sc.textFile('kindness.txt')

lineLengths = lines.map(lambda s: len(s))

# collect() is an action
lineLengths.collect()

# avoid collect on large datasets, it will return all elements of rdd
# gives the first value, first() is an action
lineLengths.first()
# returns an array with the first n elements of the dataset
# take() is an action
lineLengths.take(10)
# returns the number of elements in the dataset
# count() is an action
lineLengths.count()

# return the first n elements of the RDD using their natural order
# takeOrdered() is an action 
lineLengths.takeOrdered(44)


# if you want to use lineLengths again later, use persist() to save it in memory
lineLengths.persist()

# you can change it to unpersist()
lineLengths.unpersist()

# this will aggregate (add) the values to give a total sum
# reduce takes 2 arguments and returns 1 argument
# reduce() is an action
totalLength = lineLengths.reduce(lambda a, b: a + b)

# returns a discrete value
totalLength


# foreach() is an action
# foreach() applies a function to all elements of an RDD
def printfunc(x): print(x)

lorem = sc.textFile('lorem.txt')
lorem.collect()

words = lorem.flatMap(lambda x: x.split())
words.take(20)
words.count()

words.foreach(lambda x: printfunc(x))








