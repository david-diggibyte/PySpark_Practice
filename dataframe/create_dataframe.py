# import sparksession

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("create dataframe").getOrCreate()
data = [(1,'david',22,"male"),(2,'susai',23,'male'),(3,'sathiya',21,'female'),(4,'anand',26,'male')]
schema = ('ID','NAME','AGE','GENDER')
dataframe = spark.createDataFrame(data=data,schema=schema)
dataframe.show()
print(dataframe.take(2))