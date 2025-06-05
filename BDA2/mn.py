import logging
logging.getLogger("py4j").setLevel(logging.ERROR)

from pyspark import SparkContext

sc = SparkContext("local", "MatrixMultiplication")
sc.setLogLevel("ERROR")  # Suppresses INFO/WARN logs

# Matrix A: 2D list of rows
A = [[1, 2], [3, 4], [5, 6]]

# Vector b (as column vector)
b = [10, 20]

# Parallelize A and b
A_rdd = sc.parallelize(A)
b_broadcast = sc.broadcast(b)

# Multiply A * b
result = A_rdd.map(lambda row: sum([x * y for x, y in zip(row, b_broadcast.value)]))

print("Result vector A * b:", result.collect())

sc.stop()

