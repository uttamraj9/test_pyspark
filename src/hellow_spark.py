from pyspark.sql import SparkSession
import random

# Create a SparkSession
spark = SparkSession.builder.appName("SparkPi").getOrCreate()

# Define the number of samples to use for the calculation
num_samples = 1000000

# Function to generate a random point within the unit square
def inside_unit_circle(_):
    x = random.random()
    y = random.random()
    return 1 if x ** 2 + y ** 2 <= 1 else 0

# Create an RDD of random points and count the number of points inside the unit circle
count = spark.sparkContext.parallelize(range(0, num_samples)).map(inside_unit_circle).reduce(lambda a, b: a + b)

# Estimate the value of pi
pi = 4.0 * count / num_samples

# Print the estimated value of pi
print("Pi is roughly", pi)

print("Pi is roughly new print with github webhook", pi)

# Stop the SparkSession
spark.stop()
