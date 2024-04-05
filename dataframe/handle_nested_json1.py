from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,ArrayType
spark = SparkSession.builder.appName('Handle nested JSON ').getOrCreate()

schema = StructType([
    StructField('employees', ArrayType(StructType([
             StructField('empId', IntegerType(), True),
             StructField('empName', StringType(), True)])), nullable=True),
    StructField('id', IntegerType(), True),
    StructField('properties', StructType([
             StructField('name', StringType(), True),
             StructField('storeSize', StringType(), True)]), nullable=True)])


df = spark.read.format('json').option('multiline',True).load(path= r'C:\Users\AKASH.S\Documents\Documents_DE\Datasets\for_nested_json_practice3.json', schema=schema)
df.show(truncate=False)
df.printSchema()
