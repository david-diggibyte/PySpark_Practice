from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, rank, dense_rank, lead, lag, asc, desc

spark = SparkSession.builder.appName('Practicing the window function ').getOrCreate()

options = {'header': True,'inferSchema': True}
df = spark.read.format('csv').options(**options).load(path=r'C:\Users\AKASH.S\Documents\Documents_DE\Datasets\for_window_fun.csv')
df.show()
df.printSchema()

# ROW_NUMBER
print('ROW_NUMBER : ')
window = Window.partitionBy('dept').orderBy(desc('salary'))

df_window_fun = df.withColumn('row number', row_number().over(window))\
               .withColumn('rank', rank().over(window))\
               .withColumn('dense_rank', dense_rank().over(window))\
               .withColumn('lag', lag('salary',1).over(window))\
               .withColumn('lead',lead('salary',1).over(window))

df_window_fun.show()