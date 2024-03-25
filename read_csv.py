from pyspark.sql import SparkSession

spark = SparkSession.builder \
        .appName("simple pyspark") \
        .getOrCreate()

df = spark.read.csv(path=r'C:\Users\AKASH.S\Documents\data.csv',header=True,inferSchema=True)
df.show()
df.printSchema()


# reading the csv file using format function

df1 = spark.read.format('csv').option(key='header',value=True).option(key='inferSchema',value=True).load(path=r'C:\Users\AKASH.S\Documents\data.csv')
df1.show()
df1.printSchema()