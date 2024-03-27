from pyspark.sql import SparkSession

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
schema = ('ID','Name','Age','Department','Salary','Company')

df = spark.createDataFrame(data=data,schema=schema)
df.show()
print(df.collect())