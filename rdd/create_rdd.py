from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("sample rdd").getOrCreate()

data = [(1,"david"),(2,"susai"),(3,"selvam")]

rdd = spark.sparkContext.parallelize(data)
print(rdd.collect())
print("number of element :",rdd.count())






