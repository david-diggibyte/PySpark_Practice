# import sparksession
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder.appName("create dataframe").getOrCreate()
data = [(1,'david',22,"male"),(2,'susai',23,'male'),(3,'sathiya',21,'female'),(4,'anand',26,'male')]
#schema = ('ID','NAME','AGE','GENDER')
schema = StructType([StructField(name = 'ID',dataType= IntegerType()),  # changing datatype using custom schema
                     StructField(name = 'NAME', dataType= StringType()),
                     StructField(name = 'AGE',dataType = IntegerType()),
                     StructField(name = 'GENDER',dataType = StringType())])
dataframe = spark.createDataFrame(data=data,schema=schema)
dataframe.show()
dataframe.printSchema()
print(dataframe.take(2))