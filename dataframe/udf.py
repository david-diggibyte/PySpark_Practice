from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, upper,col
from pyspark.sql.types import StructType, StructField, StringType

spark = SparkSession.builder.appName('performing UDF in pyspark').getOrCreate()
custom_schema = StructType([StructField('Last_name',StringType(),True),
                            StructField('First_name',StringType(),True)])
df = spark.read.format('csv').option('inferSchema',True).option('header', True).load(r'C:\Users\AKASH.S\Documents\Documents_DE\Datasets\full_name.csv')
df.show(truncate=False)
df.printSchema()

def upper_case(name):
    return name.upper()

upper_case_udf = udf(lambda x: upper_case(x))

df.withColumn('upper_first_name',upper_case_udf(col('First_name'))).show()
@udf(returnType=StringType())
def upper_case(name):
    return name.upper()

upper_case_udf = udf(lambda x: upper_case(x))

df.withColumn('First_name',upper_case(col('First_name'))).show()

df.select('First_name','Last_name',upper_case('First_name').alias('upper_name')).show()


def concat_name(first_name,last_name):
    return f'{first_name} {last_name}'

concat_name_udf = udf(lambda x,y : concat_name(x,y),returnType=StringType())

df.withColumn('full_name',concat_name_udf(col('First_name'), col('Last_name'))).show()

@udf
def concat_names(first_name,last_name):
    return f'{first_name} {last_name}'

df.select('First_name','Last_name',concat_names('First_name','Last_name').alias('Full_name')).show()