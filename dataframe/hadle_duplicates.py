from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Handle the duplicates row in DF').getOrCreate()

df = spark.read.csv(path=r'C:\Users\AKASH.S\Documents\Documents_DE\Datasets\duplicates_data.csv',header=True)
df.show(truncate=False)

# distinct

print('REMOVE DUPLICATE RPWS USING DISTINCT')
distinct_df = df.distinct()
distinct_df.show()

# using dropduplicates() function remove the all column

print('DROP DUPLICATES ALL COLUMNS : ')
drop_duplicate = df.dropDuplicates()
drop_duplicate.show()

# specify one column in dropduplicates function

print('DROP DUPLICATE USING SPECIFIC COLUMN : ')
drop_duplicate_one_col = df.dropDuplicates(['age'])
drop_duplicate_one_col.show()

# drop_duplicate function using multiple column

print('DROP DUPLICATE USING MULTIPLE COLUMN :')
drop_duplicate_mul_col = df.dropDuplicates(['name','gender','age'])
drop_duplicate_mul_col.show()

df.dropDuplicates(['age','name','id']).show()