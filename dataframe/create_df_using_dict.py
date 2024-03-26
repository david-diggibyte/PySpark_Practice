from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("create dataframe using dict").getOrCreate()

data = [{'id':1,'name':"david",'gender':'male','Education':'Bsc'},
        {'id':2,'name':'sebastin','gender':'male','Education':'Bsc'},
        {'id':3,'name':'sathiya','gender':'female','Education':'Bsc Math'}]

df = spark.createDataFrame(data=data)
df.show()
df.printSchema()