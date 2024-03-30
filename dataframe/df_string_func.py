from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName('Create dataframe ').getOrCreate()
df = spark.read.csv(path=r'C:\Users\AKASH.S\Documents\Documents_DE\Datasets\for_str_fun.csv',header=True,inferSchema=True)
df.show()

df.select('name').show()

# using upper function
#
# uname = df.withColumn('Upper_name',upper(df['name']))
# uname.show()
#
# # using lower function
#
# lname = uname.withColumn('Lower_name',lower(uname['Upper_name']))
# lname.show()
#
# # using trim, rtrim and rtrim function in DF
# #fname = lname.withColumn('name',trim(lname['name'])).show()
#
# tname = lname.withColumn('trim_name',trim(lname['name']))\
#              .withColumn('right_trim',rtrim(lname['name']))\
#              .withColumn('left_trip',ltrim(lname['name']))
# tname.show()
# # substring function using df
#
sub_string = df.withColumn('sub_string',substring(df['name'],2,5))
sub_string.show()
df.select(substring(col('name'),3,6)).show()
# substring index function

# using withcolumn
# sub_string_index1 = df.withColumn('positive substr index',substring_index(col('degree'),' ',1))
# sub_string_index1.show()
# sub_string_index2 = df.withColumn('negative substr index',substring_index(col('degree'),' ',-1))
# sub_string_index2.show()
# # using select
# df.select('*',substring_index(col('name'),' ',1).alias('sub_name')).show()

addcolum = df.withColumn('city',lit('Bangalore,coimbatore,kallakuruchi'))
addcolum.show(truncate=False)
#addcolum.withColumn('substr_index',substring_index('city',',',-1)).show()

# split function

addcolum.withColumn('spit_city',split('city',',')).show(truncate=False)
addcolum.select(split(col('city'),',',2).alias('split_city')).show(truncate=False)

