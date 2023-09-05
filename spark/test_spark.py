from pyspark.sql import SparkSession
import time
import math

def complex_function(x):
    return math.factorial(x % 10)  # O(n) complexity

def main():
    spark = SparkSession.builder \
        .appName("StressTest") \
        .getOrCreate()
    
    sc = spark.sparkContext

    large_rdd = sc.parallelize(range(0, 10_000)) 

    start_time = time.time()
    squared_rdd = large_rdd.map(complex_function)
    print("--- Map operation took %s seconds ---" % (time.time() - start_time))

    start_time = time.time()
    sum_result = squared_rdd.reduce(lambda a, b: a + b)
    print("--- Reduce operation took %s seconds ---" % (time.time() - start_time))

    start_time = time.time()
    result_rdd = large_rdd.filter(lambda x: x % 2 == 0) \
        .map(lambda x: ("even", x)) \
        .reduceByKey(lambda a, b: a + b)
    result_rdd.collect()
    print("--- Multiple transformations took %s seconds ---" % (time.time() - start_time))

    spark.stop()

if __name__ == '__main__':
    main()
