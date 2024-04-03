from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

spark = SparkSession.builder.appName('Testing interchange data ').getOrCreate()

schema = StructType([StructField('Last_Name',StringType(),True),
                    StructField('First_Name',StringType(),True)])

df = spark.read.csv(path=r'C:\Users\AKASH.S\Documents\Documents_DE\Datasets\full_name.csv',header=True,schema=schema)
df.show()
df.printSchema()