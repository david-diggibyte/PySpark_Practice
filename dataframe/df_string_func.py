from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder.appName('Create dataframe ').getOrCreate()

data = [('  david   ',' BSC CS'),
        ('k7','MSC CS  '),
        ('DiNagaran',' CSE'),
        (' Deva Sellan','Cse'),
        ('susai sebestiN','Bcom'),
        ('JOhn ','BSC cs'),
        ('chandru','B Tech'),
        ('akash','M com')]
schema = ['Name','Degree']

df = spark.createDataFrame(data = data, schema = schema)
df.show()
df.select('Name').show()
df.select(df.Degree).show()
df.select(df['Degree'],df['Name']).show()