from pyspark.sql import SparkSession
from pyspark.sql.functions import mean

spark = SparkSession.builder.appName('miss').getOrCreate()

df = spark.read.csv('./data/ContainsNull.csv',inferSchema=True,header=True)

df.printSchema()

df.show()

df.na.drop().show()

df.na.drop(thresh=2).show()

df.na.drop(how='all').show()

df.na.drop(subset=['Sales']).show()

df.na.fill('FILL VALUE').show()

df.na.fill(0).show()

df.na.fill('No Name', subset=['Name']).show()

mean_val = df.select(mean(df['Sales'])).collect()

df.na.fill(mean_val[0][0], subset=['Sales']).show()
