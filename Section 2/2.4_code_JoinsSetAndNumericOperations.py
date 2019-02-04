


# Joins

rdd1 =  sc.parallelize([("foo", 1), ("bar", 2), ("baz", 3), ("fred", 4)])
rdd2 =  sc.parallelize([("foo", 5), ("bar", 6), ("bar", 7), ("waldo", 8)])

# inner join
rdd1.join(rdd2).collect()

# left outer join
rdd1.leftOuterJoin(rdd2).collect()

# right outer join
rdd1.rightOuterJoin(rdd2).collect()

# full outer join
rdd1.fullOuterJoin(rdd2).collect()


# Example comes from pages 220-224 in  book Apache Spark in 24 Hours.
stores = sc.parallelize(['100\tBoca Raton', '101\tColumbia', '102\tCambridge', '103\tNaperville'])\
           .map(lambda x: x.split('\t')) \
           .keyBy(lambda x: x[0]) \



salespeople = sc.parallelize(['1\tHenry\t100', '2\tKaren\t100', '3\tPaul\t101', '4\tJimmy\t102', '5\tJanice\t']) \
				.map(lambda x: x.split('\t')) \
				.keyBy(lambda x: x[2])


# Inner join (or join) returns all elements from both datasets with 
# matching keys.
inner = salespeople.join(stores).collect()

# Left Outer Join returns all records from the left dataset along 
# with matched records by key from the right dataset.
salespeople.leftOuterJoin(stores).collect()

left = salespeople.leftOuterJoin(stores)\
			.filter(lambda x: x[1][1] is None) \
			.map(lambda x: "salesperson " + x[1][0][1] + " has no store") \
			.collect()
left

# Right Outer Join returns all records from the right dataset along 
# with matched records by key from the left dataset.
salespeople.rightOuterJoin(stores).collect()

right = salespeople.rightOuterJoin(stores)\
			.filter(lambda x: x[1][0] is None) \
			.map(lambda x: x[1][1][1] + " store has no salespeople") \
			.collect()
right

# Full outer join
salespeople.fullOuterJoin(stores).collect()

full = salespeople.fullOuterJoin(stores) \
			.filter(lambda x: x[1][0] is None or x[1][1] is None) \
			.collect()
full

# performing set operations
rdd_one = sc.parallelize([1, 7, 3, 45, 66, 90, 1024])
rdd_two = sc. parallelize([1, 3, 8, 45, 64, 90, 512])

# intersection
rdd_one.intersection(rdd_two).collect()

# subtraction
rdd_one.subtract(rdd_two).collect()


# union
rdd_one.union(rdd_two).collect()


# distinct
rdd_three = sc.parallelize(["red", "red", "red", "blue", "blue", "green", "yellow", "yellow", "yellow"])
rdd_three.distinct().collect()


# Numeric operations

numbers = sc.parallelize([0,1,1,2,3,5,8,13,21,34])
numbers.min()
numbers.max()
numbers.mean()
numbers.sum()
numbers.stdev()
numbers.variance()
numbers.stats()

