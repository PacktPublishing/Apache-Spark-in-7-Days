
# shared variables


# broadcast variable is read-only on the worker nodes
# broadcast example

broadcastVar = sc.broadcast([1, 2, 3])
broadcastVar.value


# example of large configuration dictionary or lookup table
config = sc.broadcast({"transformation": 13, "action": True})
config
config.value


# accumulator variable is write-only on the worker nodes
# accumulator example
accum = sc.accumulator(0)
accum
def test_accum(x):
	accum.add(x)


# foreach is a transformation and it does not return a value, it only executes the function on each element
sc.parallelize([1, 2, 3, 4]).foreach(lambda x: accum.add(x))
accum.value
