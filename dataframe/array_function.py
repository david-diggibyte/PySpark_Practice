from pyspark.sql import SparkSession
from pyspark.sql.functions import array, array_contains, array_size, array_position, array_remove

spark = SparkSession.builder.appName('For Practicing array function').getOrCreate()

options = {'header': True, 'inferSchema': True, 'delimiter': '-'}

df = spark.read.format('csv').options(**options).load(path= r'C:\Users\AKASH.S\Documents\Documents_DE\Datasets\for_array_fun.csv')
df.show()
df.printSchema()

# ARRAY FUNCTION ----> array it's used to combining multiple column into one array

print('ARRAY FUNCTION :')
df_array = df.withColumn('array_name',array(df.name, df.id))
df_array.show(truncate=False)
df_array.printSchema()

# ARRAY CONTAINS ----> it's used to check the array element if it is element is there return true else false

print('ARRAY_CONTAINS FUNCTION :')
df_contains = df_array.withColumn('array_contains',array_contains(df_array.array_name,'1'))
df_contains.show()

# ARRAY_SIZE or SIZE (ARRAY_LENGTH) ---> it's return the how many elements is there in the array

print('ARRAY LENGTH :')
array_length = df_contains.withColumn('array_length',array_size(df_contains.array_name))
array_length.show()

# ARRAY_POSITION  ---> it's return the occurrence of the element in the array

print('ARRAY_POSITION :')
array_position = array_length.withColumn('array_position', array_position(array_length.array_name, 'david'))
array_position.show()

# ARRAY_REMOVE -----> it's used to remove the element in the array

print('ARRAY_REMOVE FUNCTION:')
array_remove = array_position.withColumn('array_remove', array_remove(array_position.array_name, 'david'))
array_remove.show()

