from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce
spark = SparkSession.builder.appName('handle null/none value in df').getOrCreate()

# df = spark.read.csv(path=r'C:\Users\AKASH.S\Documents\Documents_DE\Datasets\contain_null_data.csv',header=True)
# df.show()
# df.printSchema()
#
# # filtering the NULL using filter and isNotNull
#
# df_filter_null = df.filter(df.id.isNotNull() & df.gender.isNotNull())
# df_filter_null.show()

data = [(None,'david',22,'male'),
        (2,'kesavan',None,'male'),
        (3,None,24,'male'),
        (4,'sebastin',35,None),
        (5,'susai',34,'male')]

schema = ['id','name','age','gender']

df1 = spark.createDataFrame(data=data,schema=schema)
df1.show()

#handling null using filter and isNotNull

df1_filter_null = df1.filter(df1.id.isNotNull())
df1_filter_null.show()

# multiple column
df1.filter(df1.name.isNotNull() & df1.age.isNotNull() & df1.id.isNotNull()).show()

#   Replacing null using fillna

print('FILLNA :')
df1_replace = df1.fillna(0)
df1_replace.show()

# replacing specific column

print('FILL SPECIFIC COLUMN USING FILL :')
df1.fillna({'name': 'ddddd','gender': 'female'}).show()

#Using Coalesce

print('USING COALESCE')
df1_coalesce = df1.withColumn('coalesce',coalesce(df1.id, df1.name,df1.age, df1.gender))
df1_coalesce.show()

# na.drop  ---> remove the contain null rows in  DF
print('NA.DROP :')
df1_na_drop = df1.na.drop()
df1_na_drop.show()

df1.dropna(subset=['id']).show()




