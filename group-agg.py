from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, stddev, countDistinct, format_number

spark = SparkSession.builder.appName('aggs').getOrCreate()

df = spark.read.csv('./data/sales_info.csv',inferSchema=True,header=True)

df.printSchema()

df.show()

df.groupBy('Company').mean().show()

df.groupBy('Company').count().show()

df.agg({'Sales': 'sum'}).show()

group_data = df.groupBy('Company')
group_data.agg({'Sales': 'max'}).show()

df.select(countDistinct('Company')).show()

df.select(avg('Sales').alias('Average Sales')).show()

df.select(stddev('Sales').alias('Std Dev')).show()

sales_std = df.select(stddev('Sales').alias('Std Dev')).select(format_number('Std Dev', 2)).show()

df.orderBy('Sales').show()

df.orderBy(df['Sales'].desc()).show()
