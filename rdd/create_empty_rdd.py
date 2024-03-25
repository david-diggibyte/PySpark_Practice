from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('empty rdd').getOrCreate()

erdd = spark.sparkContext.emptyRDD()
erdd2 = spark.sparkContext.parallelize([])
print(erdd.isEmpty())
print(erdd2.collect())