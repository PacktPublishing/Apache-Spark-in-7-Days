
# Use the rdd below for questions 1-3.
nums = sc.parallelize([4, 20, 20, 55, 64, 64, 1024, 1024, 3, 7, 10, 10, 99])

# Question 1. Return the first element.
# Question 2. Return the first five elements.
# Question 3. Return all the elements.


# Use the rdd below for question 4.
locations = sc.parallelize([('Niceville', 'USA', 1)
	,('Berlin', 'Germany', 2)
	,('Sunnyvale', 'USA', 3)
	,('Buenos Aires', 'Argentia', 4)])

# Question 4.  Fill in keyBy() to set a designated key to be the third element (2nd index)
# and return all the elements. See template below.

bykey = locations.____(lambda x: x[2])
bykey.collect()


# For question 5, take a look at this example, which uses the map function
x_one = [1,2,3,4,5,6,7,8,9]
rdd_one = sc.parallelize(x_one)
series_one = rdd_one.map(lambda x: [x, (x+1)*x])
series_one.collect()

# Question 5.  Now, replace map with flatMap() to compare the difference.
# What is the difference in output between map() and flatMap()?


# For questions 6-10, read in the text file, shakespeare.txt
doc = sc.textFile("shakespeare.txt")

# Question 6. Use a flatMap operation to tokenize (split up the words)
# then use a map function to create a count for each word, (x, 1)
# finally, aggregate the values per key, using reduceByKey operation
# that includes (lambda x, y: x + y).  Store the rdd in a variable called words.

# Question 7.  To save the calculation, use cache() on words.
# Question 8.  Count the elements, using count(). Notice it triggers computation.
# Question 9.  Return the first five results, using take().
# Question 10. Count the elements, using count() again.  Notice no computation is required.


# For questions 11-18, read in the shakespeare.txt document and save the rdd as doclit.

import re
doclit = sc.textFile("shakespeare.txt")
doclit


# Question 11.  Filter empty lines from the rdd, using filter(), split lines by whitespace,
# and flatten the lists of words into one list, using flatMap(). Save rdd result as flattened.

# Question 12. Inspect the rdd by returning the first 20 elements.

# Question 13.  Filter out words with 0 length and then convert the rdd
# to key-value pairs in the form of (word, 1), using map(). Save the new
# rdd as kvpairs.

# Question 14.  Inspect the rdd, using take(20) to see the first 20 elements.

# Question 15.  Use reduceByKey(), to count each word and sort the results in reverse alphabetical order
# using sortByKey(ascending=False).  Save rdd in countsbyword.

# Question 16.  Inspect the rdd, use countsbyword.take(50)

# Question 17.  Display the top key value pair to make the count the key and sort
# invert the key value pair to make the count the key and sort. 
# Use map(lambda (w,c): (c,w)) and sortByKey(ascending=False).  Save rdd in
# topwords.

# Question 18.  Display the top 20 most used words, using take(20)



# For question 19, use the following rdds
one = sc.parallelize(range(1,10))
two = sc.parallelize(range(5,15))
# Question 19.  Use intersection() and collect().


# For question 20, use the following rdds
one = sc.parallelize(range(1,10))
two = sc.parallelize(range(10,21))
# Question 20. Use union() and collect()

# For question 21, use the following rdds 
one = sc.parallelize(range(1,9))
two = sc.parallelize(range(5,15))
# Notice the result of this:
one.union(two).distinct().collect()

# Question 21.  Use union(), distinct() and top(10)


# For question 22, use the following rdd
vals = sc.parallelize([2, 4, 6, 8, 10, 12, 14, 16])

# Question 22. Find the min(), max(), mean(), and stats on this rdd.




















