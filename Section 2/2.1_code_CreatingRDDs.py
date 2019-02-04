
# Examples of using pyspark shell with basic operations

# print()
print("Hello, World!")

# addition
-4 + 77

# subtraction
80 - 20

# float division
50.0 / 11.0

# modulo division
100 % 7

# floor division
50 // 11

# exponents
2**20

# logical operators
a = False
b = True

print((not a) and b)  # True and True gives True
print(b or a)
print((not b) or a)

# set operations
numtuple = (42, 64, 77, 1048, 5, 95)
print(2 in numtuple)
print(64 in numtuple)

# Example using a function
def myfunc(val):
	return val*val

myfunc(2)
result = myfunc(200)
print(result)

# example of an RDD by distributing a collection of objects
seasonsrdd = sc.parallelize(["autumn", "winter", "spring", "summer"])
seasonsrdd.first()

# notice the use of the parallelize() method
numsrdd = sc.parallelize([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10])

# gives type of RDD created
numsrdd

# gives the min value
numsrdd.min()

# gives the max value
numsrdd.max()

# return the parallel collection as a list
# collect will fetch the entire rdd to a single machine 
numsrdd.collect()

# Create an rdd using the range() method
rangerdd = sc.range(0, 1000, 1, 2)
rangerdd
# find number of partitions (numSlices=2)
rangerdd.getNumPartitions()
rangerdd.min()
rangerdd.max()
rangerdd.take(10)

# create an RDD by loading an external file
lines = sc.textFile('kindness.txt')
lines.count()
lines.take(15)
lines.take(44)


# Count the number of times "kindness" appears in text
lines.filter(lambda line: "kindness" in line.lower()).count()


