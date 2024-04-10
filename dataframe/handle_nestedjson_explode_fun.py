from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, posexplode_outer, posexplode, explode_outer, ArrayType
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.appName('handle nested jason and explode function').getOrCreate()
schema = StructType([
                     StructField('id', IntegerType(), True),
                     StructField('name', StringType(), True),
                     StructField('projects', ArrayType(StructType([
                                StructField('project_name', StringType(), True),
                                StructField('tasks', ArrayType(StringType(), True), nullable=True)
                     ]), True), nullable=True),
                     StructField('skills', ArrayType(StringType(), True), True)])

df = spark.read.format('json').option('multiline', True).load(path=r'C:\Users\AKASH.S\Documents\Documents_DE\Datasets\for_nested_json_practice4.json', schema=schema)
#
# df.show(truncate=False)
# df.printSchema()

explode1 = df.select('id', 'name', explode('projects').alias('projects'), 'skills')
explode1.show(truncate=False)

df1 = explode1.select('id','name','projects.*','skills')
df1.show(truncate=False)

print('EXPLODE:')

explode_df1 = df1.select('id', 'name', 'project_name', explode('tasks').alias('task'), 'skills')
explode_df1.show(truncate=False)

print('EXPLODE_OUTER:')
explode_outer_df1 = df1.select('id', 'name', 'project_name', explode_outer('tasks').alias('task'), 'skills')
explode_outer_df1.show(truncate=False)

print('POSEXPLODER:')

posexplode_df1 = df1.select('id', 'name', 'project_name', posexplode('tasks').alias('position', 'task'), 'skills')
posexplode_df1.show(truncate=False)

print('POSEXPLODER_OUTER:')

posexplode_outer_df1 = df1.select('id', 'name', 'project_name', posexplode_outer('tasks').alias('position','task'), 'skills')
posexplode_outer_df1.show(truncate=False)