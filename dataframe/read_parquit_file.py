from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName('Read file parquet').getOrCreate()

df = spark.read.parquet(r'C:\Users\AKASH.S\Documents\Documents_DE\Datasets\MT cars.parquet')
df.show()
df.printSchema()
print(df.count())