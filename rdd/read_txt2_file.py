from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('reading a txt file').getOrCreate()

rdd = spark.sparkContext.textFile(name=r'C:\Users\AKASH.S\Documents\check_in.txt')

print(rdd.collect())
print(rdd.count())

# Spliting the word using space in lambda function and split

rdd2 = rdd.flatMap(lambda x : x.split(' '))
print(rdd2.collect())
print(rdd2.count())
print('rdd 2 min :',rdd2.min())
print('rdd 2 max :',rdd2.max())

# assign one each word using lambda and map

rdd3 = rdd2.map(lambda x: (x, 1))
print(rdd3.collect())

# checking the sama word or not if same word aggregate

rdd4 = rdd3.reduceByKey(lambda x, y: x+y)
print(rdd4.collect())

# soring the value by key using reduce by key function

rdd5 = rdd4.map(lambda x: (x[1], x[0])).sortByKey()
print(rdd5.collect())