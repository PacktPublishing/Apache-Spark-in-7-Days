
# Example of actions
nums = sc.parallelize([4, 20, 20, 55, 64, 64, 1024, 1024, 3, 7, 10, 10, 99])
# Question 1. return the first element of the rdd
nums.first()
# Question 2. return the first 5 elements of the rdd
nums.take(5)
# Question 3. return all the elements of the rdd
nums.collect()


# Example of using indexing and setting a designated key
locations = sc.parallelize([('Niceville', 'USA', 1)
	,('Berlin', 'Germany', 2)
	,('Sunnyvale', 'USA', 3)
	,('Buenos Aires', 'Argentia', 4)])

# set a designated key to be the third element and return all the elements per key
# Question 4
bykey = locations.keyBy(lambda x: x[2])
bykey.collect()


# Take a look at this example of a map function
x_one = [1,2,3,4,5,6,7,8,9]
rdd_one = sc.parallelize(x_one)
series_one = rdd_one.map(lambda x: [x, (x+1)*x])
series_one.collect()

# Now, use flatMap and see how it compares to the map result
# (result is a flattened list, compare to map)
# Question 5
x_two = [1,2,3,4,5,6,7,8,9]
rdd_two = sc.parallelize(x_two)
series_two = rdd_two.flatMap(lambda x: [x, (x+1)*x])
series_two.collect()

# read in the text file, shakespeare.txt
doc = sc.textFile("shakespeare.txt")

# use a flatMap operation to tokenize (split up the words)
# then use a map function to create a count for each word, like (x, 1)
# finally, aggregate the values per key, using reduceByKey operation
# Question 6.
words = doc.flatMap(lambda x: x.split()) \
		   .map(lambda x: (x, 1)) \
		   .reduceByKey(lambda x, y: x + y)

# save the calculation, using cache()
# Question 7.		   
words.cache()

# count the elements, using count(), notice it triggers computation
# Question 8.
words.count() # triggers computation

# take the first five results
# Question 9.
words.take(5)

# count the elements again, using count(), note it does not trigger computation
# Question 10.
words.count() # no computation required



# import regular expression module
import re
# load the file, shakespeare.txt
doclit = sc.textFile("shakespeare.txt")
# inspect rdd created
doclit

# Filter empty lines from the RDD, split lines by whitespace,
# and flatten the lists of words into one list.
# Use the filter function to remove lines with no text
# Questions 11.
flattened = doclit.filter(lambda line: len(line) > 0) \
				.flatMap(lambda line: re.split('\W+', line))

# inspect the RDD by returning the first 20 elements, use take()
# Question 12.
flattened.take(20)

# map text to lowercase, remove empty strings, and 
# then convert to key value pairs in the form of (word, 1)
# Question 13.
kvpairs = flattened.filter(lambda word: len(word) > 0) \
					.map(lambda word: (word.lower(), 1))

# inspect the RDD, using take(20). it should be a pair RDD.
# Question 14.
kvpairs.take(20)

# count each word and sort results in reverse alphabetic order:
# Question 15.
countsbyword = kvpairs.reduceByKey(lambda v1, v2: v1 + v2) \
					  .sortByKey(ascending=False)
# inspect the RDD, use take(50)
# Question 16.
countsbyword.take(50)

# Display the top kv pair to make the count the key and sort
# invert the kv pair to make the count the key and sort
# Question 17.
topwords = countsbyword.map(lambda (w,c): (c,w)) \
					  .sortByKey(ascending=False)

# Display the top 20 most used words
# Question 18.
topwords.take(20)


# use of intersection()
# Question 19.
one = sc.parallelize(range(1,10))
two = sc.parallelize(range(5,15))
one.intersection(two).collect()

# use of union()
# Question 20.
one = sc.parallelize(range(1,10))
two = sc.parallelize(range(10,21))
one.union(two).collect()

# use of distinct()
one = sc.parallelize(range(1,9))
two = sc.parallelize(range(5,15))
one.union(two).distinct().collect()
# Question 21
one.union(two).distinct().top(10)

# Use min(), max(), mean(), and stats() on this rdd
# Question 22
vals = sc.parallelize([2, 4, 6, 8, 10, 12, 14, 16])
vals.min()
vals.max()
vals.mean()
vals.stats()





