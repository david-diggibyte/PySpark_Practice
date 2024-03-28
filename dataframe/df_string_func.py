from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName('Create dataframe ').getOrCreate()
df = spark.read.csv(path=r'C:\Users\AKASH.S\Documents\for_str_fun.csv',header=True,inferSchema=True)
df.show()

df.select('name').show()

# using upper function

uname = df.withColumn('Upper_name',upper(df['name']))
uname.show()

# using lower function

lname = uname.withColumn('Lower_name',lower(uname['Upper_name']))
lname.show()

# using trim, rtrim and rtrim function in DF

tname = lname.withColumn('trim_name',trim(lname['name']))\
             .withColumn('right_trim',rtrim(lname['name']))\
             .withColumn('left_trip',ltrim(lname['name']))
tname.show()
# substring function using df

sub_string = df.withColumn('sub_string',substring(df['name'],2,5))
sub_string.show()
