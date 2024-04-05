from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, explode_outer, posexplode, posexplode_outer
from pyspark.sql.types import StructType, StructField, ArrayType, StringType, IntegerType

spark = SparkSession.builder.appName('HANDLE NESTED JSON FILE AND EXPLODE ARRAY FUNCTION').getOrCreate()
custom_schema = StructType([(StructField('countries', ArrayType(StructType([
                                                            (StructField('code', StringType(), True)),
                                                            (StructField('name', StringType(), True)),
                                                            (StructField('rank', IntegerType(), True))])), nullable=True))])
#df = spark.read.json(path=r'C:\Users\AKASH.S\Documents\Documents_DE\Datasets\for_nested_json_practice2.json', schema=custom_schema,multiLine=True)

df = spark.read.format('json').option('multiline', True).load(path=r'C:\Users\AKASH.S\Documents\Documents_DE\Datasets\for_nested_json_practice2.json', schema=custom_schema)

df.show(truncate=False)
df.printSchema()

print('EXPLODE_COUNTRIES: ')
explode_countries = df.select(explode('countries').alias('countries'))
explode_countries.show(truncate=False)

print('POSEXPLODE_COUNTRIES: ')
explode_countries = df.select(posexplode('countries').alias('pos', 'countries'))
explode_countries.show(truncate=False)

print('EXPLODE_OUTER: ')
explode_outer = df.select(explode_outer('countries').alias('countries'))
explode_outer.show(truncate=False)

print('POSEXPLODER_OUTER:')
posexploder_outer = df.select(posexplode_outer('countries').alias('pos','countries'))
posexploder_outer.show(truncate=False)

print('BREAK THE DICTIONARY:')
break_dict = explode_countries.select('countries.*').na.drop()
break_dict.show(truncate=False)