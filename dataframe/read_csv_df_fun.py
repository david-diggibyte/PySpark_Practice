from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Read csv file for dataframe function practice').getOrCreate()
df = spark.read.csv(path = r'C:\Users\AKASH.S\Documents\employee_data.csv',header=True,inferSchema=True)
df.show()
df.printSchema()
print(df.collect())
print(df.take(2))
print(df.count())
df.select('Name','Department').show()
df.select(df.ID,df.Name,df.Company).show()
df.select('*').show()
df.filter((df['Salary']>10000) | (df['Company']=='diggibyte')).show()
df.where((df['Salary']>10000) & (df['Company'] == 'diggibyte')).show()
df.filter(df['Name'].like('_a%')).show()
df.sort('Salary', ascending=False).show()
df.describe().show()
print(df.columns)