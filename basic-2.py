from pyspark.sql import SparkSession
from pyspark.sql.types import (StructField,
    StringType,
    IntegerType,
    StructType)

spark = SparkSession.builder.appName('Basic-2').getOrCreate()

schema = StructType(fields=[StructField('age', IntegerType(), True), StructField('name', StringType(), True)])

df = spark.read.json('./data/people.json', schema=schema)

df.select('age').show()

print(df.head(2))

df.withColumn('newage', df['age']).show()

df.createOrReplaceTempView('people')
results = spark.sql('SELECT * FROM people')
results.show()

results = spark.sql('SELECT * FROM people WHERE age=30')
results.show()
