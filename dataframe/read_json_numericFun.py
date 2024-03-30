from pyspark.sql import SparkSession
from pyspark.sql.functions import sum,min,max,abs,avg,round,col,count,when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.appName('Read JSON file amd numeric function').getOrCreate()

# using custom schema define datatype and column
schema = StructType([StructField('name', StringType(), True),
                     StructField('age', IntegerType(), True),
                     StructField('city', StringType(), True),
                     StructField('department', StringType(), True),
                     StructField('gender', StringType(), True),
                     StructField('salary', IntegerType(), True)
                     ])

df = spark.read.json(path=r'C:\Users\AKASH.S\Documents\Documents_DE\Datasets\employee.json',schema=schema)
df.show()
df.printSchema()

# numeric function

# SUM FUNCTION

df.select(sum(df.salary).alias('sum_salary')).show()
df.groupBy('city').agg(sum('salary').alias('total_salary')).show()

# AVG FUNCTION

df.select(avg(df.salary).alias('avg_salary')).show()
df.groupBy('department').agg(avg('salary').alias('avg_salary'),count('department').alias('count_dept')).show()
df.groupBy('department').agg(avg('salary').alias('avg_salary'),count('department').alias('count_dept')).filter(col('count_dept')>1).show()

# MIN FUNCTION

df.select(min('age').alias('min_age')).show()
df.groupBy('name').min('age').alias('min_age').show()

#MAX FUNCTION

df.select(max(df.age)).show()

# ROUND FUNCTION

df.select(round(avg('salary'),3).alias('round_salary')).show()

#absolute function

df.select(abs(min('salary')).alias('NtoP')).show()