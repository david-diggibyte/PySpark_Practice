from pyspark.sql import SparkSession
from pyspark.sql.functions import col
spark = SparkSession.builder.appName('performing joins ').getOrCreate()
df1 = spark.read.csv(path=r'C:\Users\AKASH.S\Documents\Documents_DE\Datasets\join1.csv',header=True)
df1.show(truncate=False)
df2 = spark.read.csv(path=r'C:\Users\AKASH.S\Documents\Documents_DE\Datasets\join2.csv',header=True)
df2.show()
df1.printSchema()
df2.printSchema()

# INNER JOIN
print('INNER JOIN :')
inner_join = df1.join(df2,df2.company_id == df1.company_id,'inner')
inner_join.show(truncate=False)

print('INNER JOIN WITH SPECIFIC COLUMNS:')
specific_join = df1.join(df2,df1.company_id == df2.company_id,'inner').select(col('name'),col('age'),col('company_name'))
specific_join.show(truncate=False)


# LEFT JOIN
print('LEFT JOIN :')
left_join = df1.join(df2,df1.company_id == df2.company_id,'left')
left_join.show(truncate=False)

# RIGHT JOIN
print('RIGHT JOIN :')
right_join = df1.join(df2,df1.company_id == df2.company_id,'right')
right_join.show(truncate=False)

# FULL JOIN
print('FULL JOIN : ')
full_join = df1.join(df2,df1.company_id == df2.company_id,'full')
full_join.show(truncate=False)

# LEFT SEMI JOIN
print('LEFT SEMI JOIN : ')
left_semi_join = df1.join(df2,df1.company_id == df2.company_id,'semi')
left_semi_join.show(truncate=False)

# LEFT ANTI JOIN
print('LEFT ANTI JOIN :')
left_anti_join = df1.join(df2,df1.company_id == df2.company_id, 'anti')
left_anti_join.show(truncate=False)

# CROSS JOIN
print('CROSS JOIN :')
cross_join = df1.crossJoin(df2)
cross_join.show(n=100,truncate=False)

