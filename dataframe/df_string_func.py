from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName('Create dataframe ').getOrCreate()
df = spark.read.csv(path=r'C:\Users\AKASH.S\Documents\Documents_DE\Datasets\for_str_fun.csv',header=True,inferSchema=True)
df.show()

df.select('name').show()

#using upper function

uname = df.withColumn('Upper_name',upper(df['name']))
uname.show()

# using lower function

lname = uname.withColumn('Lower_name',lower(uname['Upper_name']))
lname.show()

# using trim, rtrim and rtrim function in DF
print('TRIM FUNCTION :')
name = lname.withColumn('name',trim(lname['name'])).show()

tname = lname.withColumn('trim_name',trim(lname['name']))\
             .withColumn('right_trim',rtrim(lname['name']))\
             .withColumn('left_trip',ltrim(lname['name']))
tname.show()
# substring function using df
print('SUBSTRING FUNCTION')
sub_string = df.withColumn('sub_string',substring(df['name'],2,5))
sub_string.show()
df.select(substring(col('name'),3,6)).show()
# substring index function

# using withcolumn
print('SUBSTRING_INDEX FUNCTION ')
sub_string_index1 = df.withColumn('positive substr index',substring_index(col('degree'),' ',1))
sub_string_index1.show()
sub_string_index2 = df.withColumn('negative substr index',substring_index(col('degree'),' ',-1))
sub_string_index2.show()

# using select
df.select('*',substring_index(col('name'),' ',1).alias('sub_name')).show()

addcolum = df.withColumn('city',lit('Bangalore,coimbatore,kallakuruchi'))
print('ADDED ONE COLUMN CITY USING LITERAL FUN :')
addcolum.show(truncate=False)
addcolum.withColumn('substr_index',substring_index('city',',',-1)).show()

# split function
print('SPLIT FUNCTION :')
addcolum.withColumn('spit_city',split('city',',')).show(truncate=False)
addcolum.select(split(col('city'),',',2).alias('split_city')).show(truncate=False)

#repeat function

# using select
print('REPEAT FUNCTION ')
df.select('name',repeat(col('name'),4).alias('repeat_name')).show(truncate=False)
# using withcolumn
df.withColumn('name',repeat(col('name'),5)).show(truncate=False)

# pad function
print('PAD FUNCTION ')
df.select('name',rpad(col('name'),20,'!').alias('rpad_name')).show(truncate=False)
df.withColumn('usingrpad',rpad(col('name'),20,'!')).withColumn('usinglpad',lpad(col('name'),15,'!')).show(truncate=False)

# regex_replace function
print('REGEXP_REPLACE FUNTION')
df.withColumn('regex_func',regexp_replace(col('name'),' ','S')).show(truncate=False)
df.select('name',regexp_replace(col('name'),'D','S')).show(truncate=False)

# regex_extract function
print('REGEXP_EXTRACT FUNCTION')
df.select('degree',regexp_extract(col('degree'),r',\s*([^ ]+)',1).alias('regex_extract_city')).show()
df.withColumn("degree_level", regexp_extract(col("degree"), r'^([^ ]+)',1)).show()

# lower and upper in DF
print('LOWER AMD UPPER FUNCTION')
df.select('name',lower(col('name')).alias('lower name'),upper(col('name').alias('upper_name'))).show()
df.withColumn('lower_name',lower(col('name'))).withColumn('upper_name',upper(col('name'))).show()


# length function
print('LENGTH FUNCTION')
df.select('name',length('name').alias('len_name'),'degree',length('degree').alias('len_degree')).show()

