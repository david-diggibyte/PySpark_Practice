from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('read csv file').getOrCreate()

dataframe = spark.read.csv(path=r'C:\Users\AKASH.S\Documents\data.csv',header=True,inferSchema=True)
dataframe.show()
dataframe.printSchema()

