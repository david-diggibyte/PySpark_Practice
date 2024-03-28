from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName('practicing withcolumn function').getOrCreate()

df = spark.read.csv(path=r'C:\Users\AKASH.S\Documents\for_str_fun.csv',header=True)
df.show()
df.printSchema()
df.select(col('degree')).show()  # select column using col function

#  add one column using withcolumn

add_c = df.withColumn('age',lit(22))
add_c.show()

add_mc = add_c.withColumn('company',lit('Diggibyte'))\
               .withColumn('slary',lit(10000))   # add multiple column
add_mc.show()

# adding new column from exiting one

df1 = add_mc.withColumn('Bonus',col('slary')*2)\
             .withColumn('AGE',add_mc['age']+2)  # using both ways to adding new column from exiting one column
df1.show()

df1.printSchema()

# using withcolumn and cast changing datatype

df2 = df1.withColumn('Bonus',col('Bonus').cast('Integer'))
df2.printSchema()
df3 = df2.withColumn('AGE',df2['AGE'].cast('Integer'))
df3.show()
df4 = df3.withColumn('Bonus',df3['Bonus'].cast('Integer'))
df4.printSchema()

# updating exiting column using withcolumn

df5 = df4.withColumn('AGE',col('AGE')*2)
df5.show()


