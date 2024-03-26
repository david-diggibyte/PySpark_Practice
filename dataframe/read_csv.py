from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('read csv file').getOrCreate()

dataframe = spark.read.csv(path=r'C:\Users\AKASH.S\Documents\data.csv',header=True,inferSchema=True)
dataframe.show()
dataframe.printSchema()

# reading the csv file using format function

df = spark.read.format('csv').option(key='header',value=True).option(key='inferSchema',value=True).load(path=r'C:\Users\AKASH.S\Documents\data.csv')
df.show()
df.printSchema()