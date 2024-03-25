from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("using range fun").getOrCreate()

rdd = spark.sparkContext.parallelize(range(11))
print(rdd.collect())

rdd1 = rdd.map(lambda x: x*2)
print(rdd1.collect())

rdd2 = rdd1.filter(lambda x : x > 4)
print(rdd2.collect())

rdd3 = rdd2.reduce(lambda x,y: x+y)
print(rdd3)