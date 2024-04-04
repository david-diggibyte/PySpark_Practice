from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('for practice union functions in df').getOrCreate()

df1 = spark.read.csv(path=r'C:\Users\AKASH.S\Documents\Documents_DE\Datasets\union1.csv',header=True,inferSchema=True)
df1.show()
df1.printSchema()

df2 = spark.read.csv(path=r'C:\Users\AKASH.S\Documents\Documents_DE\Datasets\union2.csv',header=True,inferSchema=True)
df2.show()
df2.printSchema()

# UNION FUNCTION

print('UNION and DISTINCT:')
df1_union_df2 = df1.union(df2).distinct().orderBy('id')
df1_union_df2.show()

# UNION ALL FUNCTION

print('UNION ALL:')
df1_unionall_df2 = df1.unionAll(df2).orderBy('id')
df1_unionall_df2.show()

data1 = [(1,'david'),(2,'susai'),(3,'nathan')]
df3 = spark.createDataFrame(data=data1,schema=['id','name'])
df3.show()
data2 =[('kesavan','male'),('sathiya','female'),('alex','female')]
df4 = spark.createDataFrame(data=data2,schema=['name','gender'])
df4.show()

# UNION BY NAME function  ---> union by name its check the column names it's column name is same its add otherwise add seprate column

df3_unionbyname_df4 = df3.unionByName(df4,allowMissingColumns=True)
df3_unionbyname_df4.show()
