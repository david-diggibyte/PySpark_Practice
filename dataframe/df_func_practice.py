from pyspark.sql import SparkSession
from pyspark.sql.functions import min, max, when, col, aggregate, count, sum, avg

spark = SparkSession.builder.appName('For practicing DF function').getOrCreate()

df = spark.read.csv(path=r'C:\Users\AKASH.S\Documents\Documents_DE\Datasets\employee_data2.csv',header=True,inferSchema=True)
df.show()
df.printSchema()
print(df.collect())
print(df.take(2))
print(df.columns)
df.describe().show()
print(df.count())
df.select('ID','Name','Location').show()
df.select(df['ID'],df['Name']).show()
df.select(df.columns[2:5]).show()
df.select(df.Salary,df.Name).show()
df.select(col('Name'),col('Age'),col('ID')).show()
df.select('*').show()

 # Alias function

df.select(df['Age'].alias('aage'),df['Name'].alias('Full name')).show()

# where and filter function

df.show()
df.select('Name','Age','Location').where((df['Name'] == 'David')|(df['AGE'] > 25) | (df['Location'].like('B%')) | df['Experience'].between(2,10)).show()
df.filter(df['Experience'].between(2,7)).show()

# like function

df.where(df['Name'].like('D%')).show()
df.describe(['AGE','Name']).show()

#when and otherwise in dataframe

df.withColumn('Salary category', when(col('Salary') <= 15000,'Low')
                                         .when(col('Salary') <= 35000,'Avg')
                                          .otherwise('High')).show()
df.withColumn('Salary category', when(df.Salary <= 15000,'Low')
                                         .when(df.Salary <= 35000,'Avg')
                                          .otherwise('High')).show()
df1=df.select(df.Name,df.Department,when(df.Company=='diggibyte','D').when(df.Company == 'Etech','E').otherwise('Nameless').alias('company'))
df1.show()

#order by and sort

df.orderBy(df.ID.desc()).show()
df.sort(df['Experience'].asc(),df['Salary'].asc()).show()

#group by function in DF

df.printSchema()
df.groupBy('Company').agg(count('Company').alias('Count_rows'),sum('Salary').alias('Total_salary')).show()

df.groupBy('Company').agg(count('*').alias('count_employee'), sum('Salary').alias('total salary'), min('Salary').alias('min_salary')).show()

df.groupBy(df.columns).count().where(col("count") > 1).show()

df.groupBy('Education').agg(count('Education').alias('edu')).filter(col('edu') > 6).show()

df.groupBy('Company').agg(count('Company').alias('count_company'),sum('Salary').alias('total_salary'),min('Salary').alias('min salary'),max('Salary').alias('Max_salary')).show()

df.groupBy('Department').agg(count('Department').alias('count_dept'),sum('Salary').alias('sum_salary')).filter(col('sum_salary') >= 45000).show()