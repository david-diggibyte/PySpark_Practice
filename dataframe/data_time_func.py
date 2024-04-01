from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, current_timestamp, date_add, date_diff, year, month, day, to_date, date_format
from pyspark.sql.types import StringType

spark = SparkSession.builder.appName('For practicing the data and time function ').getOrCreate()
df = spark.read.csv(path=r'C:\Users\AKASH.S\Documents\Documents_DE\Datasets\employee_data.csv',header=True,inferSchema=True)
df.show()

# add current_date

add_data = df.withColumn('current_data', current_date())
add_data.show()

# add  current_timestamp

add_timestamp = add_data.withColumn('timestamp', current_timestamp())
add_timestamp.show(truncate=False)

# date_add function

date_add = add_timestamp.withColumn('date_add',date_add('current_date',10))
date_add.show(truncate=False)

# date_diff function

date_diff = date_add.withColumn('date_diff',date_diff('date_add','current_date'))
date_diff.show(truncate=False)

# year, month and date function

ymd = date_diff.withColumn('year',year('timestamp')).withColumn('month',month('timestamp')).withColumn('day', day('timestamp'))
ymd.show(truncate=False)

ymd.printSchema()

# todate funtion -----> its used convert the date datatype

str_date = ymd.withColumn('str_date', ymd.date_add.cast(StringType()))
str_date.show()
str_date.printSchema()

convert_date = str_date.withColumn('convert_date',to_date('str_date'))
convert_date.show(truncate=False)
convert_date.printSchema()

# date_format ----> used to rearrange the date format

rearrange_date = convert_date.withColumn('rearrange_date',date_format('convert_date',"dd/MM/yyyy"))
rearrange_date.show(truncate=False)