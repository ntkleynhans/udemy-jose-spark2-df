from pyspark.sql import SparkSession
from pyspark.sql.types import (StructField, StringType,
    IntegerType, StructType)

spark = SparkSession.builder.appName('Basic').getOrCreate()

df = spark.read.json('./data/people.json')

df.show()

df.printSchema()

print(df.columns)

df.describe().show()

data_schema = [StructField('age', IntegerType(), True), \
                StructField('name', StringType(), True)]

final_struct = StructType(fields=data_schema)

dfs = spark.read.json('./data/people.json',schema=final_struct)

dfs.printSchema()