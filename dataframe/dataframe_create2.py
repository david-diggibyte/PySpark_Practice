from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder.appName('Create dataframe ').getOrCreate()

data = [
        (1,'David',22,'data engineer',10000,'diggibyte'),
        (2,'K7',23,'data engineer',20000,'diggibyte'),
        (3,'Dinakaran',21,'data engineer',30000,'diggibyte'),
        (4,'sebastin',23,'fullstack',15000,'Etech'),
        (5,'kavya',22,'support',30000,'TCS'),
        (6,'Devaseelan',21,'fullstack',30000,'Etech'),
        (1,'David',22,'data engineer',10000,'diggibyte'),
        (2,'K7',23,'data engineer',20000,'diggibyte')
        ]
#schema = ('ID','Name','Age','Department','Salary','Company')
schema = StructType([StructField(name='ID',dataType =IntegerType()),
                     StructField(name='Name',dataType=StringType()),
                     StructField(name='Age',dataType=IntegerType()),
                     StructField(name='Department',dataType=StringType()),
                     StructField(name='Salary',dataType=IntegerType()),
                     StructField(name='Company',dataType=StringType())])

df = spark.createDataFrame(data=data,schema=schema)
df.show()
df.show(n=3,truncate=10,vertical=True) # with all parameter in show
print(df.take(2))    # take is used to return 2 record only
print(df.collect())   # return all record
df.printSchema()      # return the column datatype
print(df.count())     # return the column count
df.select('Name','Department').show()  # return selected column only
df.filter((df['Salary']>10000) | (df['Company']=='diggibyte')).show()  # filtering the records
df.where((df['Salary']>10000) & (df['Company'] == 'diggibyte')).show()  # filtering the record
df.filter(df['Name'].like('_a%')).show()    # filtering the record based on name have second word a only
df.sort('Salary', ascending=False).show()  # sorting descanding
df.describe().show()
print(df.columns)  # return column name only.