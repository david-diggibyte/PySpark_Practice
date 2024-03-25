# create dataframe

from pyspark.sql import SparkSession
from pyspark.sql.types import *

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Simple PySpark Example") \
    .getOrCreate()

data = [(1, "David"), (2, "Susai")]

# schema = ["id", "name"]
schema = StructType([StructField(name='id', dataType=IntegerType()),
                     StructField(name='name', dataType=StringType())])


# Create DataFrame

df = spark.createDataFrame(data=data,schema=schema)
df.show()
df.printSchema()
print(type(spark))
print(dir(spark))
print(help(StructType))

# now using dictionary to create a DataFrame

data1 = [{'id':1,'name':"david",'gender':'male','Education':'Bsc'},
         {'id':2,'name':'sebastin','gender':'male','Education':'Bsc'},
         {'id':3,'name':'sathiya','gender':'female','Education':'Bsc Math'}]
df1 = spark.createDataFrame(data=data1)
df1.show()
df1.printSchema()


