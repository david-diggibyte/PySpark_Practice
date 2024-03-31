from pyspark.sql import SparkSession
from pyspark.sql.functions import round,mean, avg, collect_list, collect_set, countDistinct, count, first, last

spark = SparkSession.builder.appName('Read Json file and performing aggregate function').getOrCreate()

df = spark.read.format('json').load(r'C:\Users\AKASH.S\Documents\Documents_DE\Datasets\employee.json')
df.show(truncate=False)

# AGGREGATE FUNCTION
# MEAN function
df.select(mean('salary').alias('mean_value')).show()
df.groupBy('gender').mean('age').alias('mean_age').show()
df.select(mean('age'),mean('salary')).show()

# AVG function
df.select(avg('salary').alias('avg_value')).show()
df.select(avg('age'),avg('salary')).show()
df.groupBy('gender').agg(round(avg('salary'),3).alias('avg_salary'),round(avg('age'),2).alias('avg_age')).show()

# COLLECT_LIST  ----> its return the all rows into list in one column
df.select(collect_list('name').alias('collect_name'),collect_list('city').alias('collect_city')).show(truncate=False)
df.groupBy('name').agg(collect_list('salary').alias('collect_salary'),collect_list('city').alias('collect_city')).show(truncate=False)

# COLLECT_SET ----> its also same like collect_list but eliminate the duplicate value

df.select(collect_set('name'),collect_set('gender')).show(truncate=False)
df.groupBy('department').agg(collect_set('name'),collect_set('city')).show(truncate=False)

# COUNT_DISTINCT FUNC ----> returns the distinct column count only not adding duplicate value

df.select(countDistinct('name'),countDistinct('gender')).show()
df.groupBy('department').agg(countDistinct('name').alias('distinct_count'),countDistinct('city').alias('distinct_count_city')).show(truncate=False)

# COUNT ----> count return the column count
df.select(count('name'),count('gender')).show()
df.groupBy('department').agg(count('name').alias('count_of_name'),count('city').alias('count_of_city')).show(truncate=False)

# FIRST  ---> return the first value in DF

df.select(first('name'),first('department')).show()
df.groupBy('gender').agg(first('name'),first('salary')).show()

# LAST  ----> return the last value in DF

df.select(last('name'),last('department')).show()
df.groupBy('city').agg(last('name').alias('last_name'),first('name').alias('first_name'),last('salary').alias('last_salary'),first('salary').alias('first_salary')).show()