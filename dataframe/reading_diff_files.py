from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
spark = SparkSession.builder.appName('Reading different files in pyspark').getOrCreate()

# custom schema

custom_schema = StructType([StructField('first_name', StringType(), True),
                            StructField('last_name', StringType(), True),
                            StructField('salary', DoubleType(), True)])


#Reading JSON file:

print('READING JSON FILE USING FORMAT METHOD:')
employee_df_json = spark.read.format('json').option('multiline', True).load(path=r'C:\Users\AKASH.S\Documents\Documents_DE\Datasets\employee_data.json', schema= custom_schema)
employee_df_json.show()
employee_df_json.printSchema()

print('READING JSON FILE USING JSON METHOD:')
employee_df_json2 = spark.read.option('multiline', True).json(path=r'C:\Users\AKASH.S\Documents\Documents_DE\Datasets\employee_data.json', schema= custom_schema)
employee_df_json2.show()
employee_df_json2.printSchema()

# Reading CSV file:

print('READING CSV FILE USING FORMAT METHOD:')
employee_df_csv = spark.read.format('csv').option('header', True).load(path= r'C:\Users\AKASH.S\Documents\Documents_DE\Datasets\employee_data1.csv', schema= custom_schema)
employee_df_csv.show()


print('READING CSV FILE USING CSV METHOD:')
employee_df_csv2 = spark.read.csv(path=r'C:\Users\AKASH.S\Documents\Documents_DE\Datasets\employee_data1.csv', header=True, schema=custom_schema)
employee_df_csv2.show()
employee_df_csv2.printSchema()

# Reading Parquet file:

print('READING PARQUET FILE USING FORMAT METHOD:')
employee_df_parquet = spark.read.format('parquet').load(r'C:\Users\AKASH.S\Documents\Documents_DE\Datasets\sample_file.parquet',schema=custom_schema)
employee_df_parquet.show()


print('READING PARQUET FILE USING PARQUET METHOD:')
employee_df_parquet2 = spark.read.schema(custom_schema).parquet(r'C:\Users\AKASH.S\Documents\Documents_DE\Datasets\sample_file.parquet')
employee_df_parquet2.show()
