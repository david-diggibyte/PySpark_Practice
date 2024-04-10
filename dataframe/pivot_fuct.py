from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, sum, avg, min, max, count
spark = SparkSession.builder.appName('performing pivot function ').getOrCreate()

data = [("Banana",1000,"USA"), ("Carrots",1500,"USA"), ("Beans",1600,"USA"),
        ("Orange",2000,"USA"),("Orange",2000,"USA"),("Banana",400,"China"),
        ("Carrots",1200,"China"),("Beans",1500,"China"),("Orange",4000,"China"),
        ("Banana",2000,"Canada"),("Carrots",2000,"Canada"),("Beans",2000,"Mexico")]

schema = ["product","price","country"]
df = spark.createDataFrame(data=data, schema=schema)
df.show(truncate=False)
df.printSchema()

pivot_df = df.groupBy('country').pivot('product').agg(sum('price'))
pivot_df.show()

df1 = spark.read.format('csv').option('header',True).option('inferSchema',True).load(path= r'C:\Users\AKASH.S\Documents\Documents_DE\Datasets\for_window_fun.csv')
df1.show(truncate=False)

print('PIVOT:')
df1.groupBy('dept').pivot('name').sum('salary').show()

print('pivot:')
df1.groupBy('name').pivot('dept').sum('salary').show()

print('pivot with specific columns :')

df.groupBy('name').pivot('dept', ['HR']).sum('salary').show()

