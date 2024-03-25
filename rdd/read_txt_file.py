from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('read txt file').getOrCreate()

rdd = spark.sparkContext.textFile(name=r'C:\Users\AKASH.S\Documents\sample1.txt')
print(rdd.collect())

rdd2 = rdd.flatMap(lambda x : x.split(' '))  # spliting the word using space
print(rdd2.collect())

rdd3 = rdd2.map(lambda x: (x,1))
print(rdd3.collect())

rdd4 = rdd3.reduceByKey(lambda x,y : x+y)  # is both same word then added
print(rdd4.collect())

rdd5 = rdd4.map(lambda x : (x[1],x[0])).sortByKey()  # sortbykey is sorting values using key
print(rdd5.collect())
count = 0
for i in rdd5.collect():
    print(i)
    count +=1
print(f'count of rdd5 : {count}')

rdd6 = rdd5.filter(lambda x: 'd' in x[1])  # only return d charactor containing words only
print(rdd6.collect())

print(f'word count : {rdd5.count()}')  # counting the records in rdd
print(f'first record :{rdd6.first()}')  # return first record in RDD
print(f'max : {rdd6.max()}')
print(f'min :{rdd6.min()}')

# totalwordcount = rdd6.filter(lambda x,y: (x[0]+y[0],x[1]))
# print(str(totalwordcount[0]))

print(f'only two record {rdd5.take(2)}')
