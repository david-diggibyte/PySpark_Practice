from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, explode_outer, posexplode_outer, posexplode
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,ArrayType
spark = SparkSession.builder.appName('Handle nested JSON and explode function').getOrCreate()

schema = StructType([
    StructField('employees', ArrayType(StructType([
        StructField('empId', IntegerType(), True),
        StructField('empName', StringType(), True)])),nullable=True),
    StructField('id', IntegerType(), True),
    StructField('properties', StructType([
        StructField('name', StringType(), True),
        StructField('storeSize', StringType(), True)]), nullable=True)])

df = spark.read.format('json').option('multiline',True).load(path= r'C:\Users\AKASH.S\Documents\Documents_DE\Datasets\for_nested_json_practice3.json',schema=schema)
df.show(truncate=False)
df.printSchema()

print('EXPLORE :')
explode_df = df.select(explode('employees').alias('employees'), 'id', 'properties')
explode_df.show(truncate=False)

print('POSEXPLODE:')
posexplode = df.select(posexplode('employees').alias('pos', 'employees'), 'id', 'properties')
posexplode.show(truncate=False)

print('EXPLORE_OUTER:')
explode_outer = df.select(explode_outer('employees').alias('employees'), 'id', 'properties')
explode_outer.show(truncate=False)

print('POSEXPLORE_OUTER:')
posexplode_outer = df.select(posexplode_outer('employees').alias('pos','employees'), 'id', 'properties')
posexplode_outer.show(truncate=False)
