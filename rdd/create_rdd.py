from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("sample rdd").getOrCreate()

data = [(1,"david"),(2,"susai"),(3,"selvam"),(4,'david')]

rdd = spark.sparkContext.parallelize(data)
print(rdd.collect())
print("number of element :",rdd.count())
print('show partitions :', rdd.getNumPartitions())
print('first element :', rdd.first())
change = rdd.map(lambda x: (x[0],x[1].upper()))
print(change.collect())

change2 = rdd.filter(lambda x: x[1].startswith('d'))
print("only start d values :")
print(change2.collect())

change3 = rdd.take(2)  # action
print(change3)

change4 = rdd.map(lambda x: x[1]).reduce(lambda x,y : x+','+y)
print(change4)

change5 = rdd.flatMap(lambda x:list(x[1])).distinct() # only distinct values
print(change5.collect())

change6 = rdd.map(lambda x : x[1]).distinct()
print(change6.collect())

change7 = rdd.zipWithIndex()  # give value with that's index also like enumerate in python
print(change7.collect())

