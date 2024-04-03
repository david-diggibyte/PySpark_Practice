from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, ArrayType, IntegerType, StringType
spark = SparkSession.builder.appName('read nested json file ').getOrCreate()

schema = StructType([StructField('countries', ArrayType(
                  StructType([StructField('name',StringType(),True),
                              StructField('code',StringType(),True),
                              StructField('rank',IntegerType(),True)])
                    ), nullable=True)
               ])
df = spark.read.option("multiline","True").json(path=r'C:\Users\AKASH.S\Documents\Documents_DE\Datasets\sample json for practice.json', schema=schema)
df.show(truncate=False)
