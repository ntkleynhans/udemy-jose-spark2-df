from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('ops').getOrCreate()

df = spark.read.csv('./data/appl_stock.csv',inferSchema=True,header=True)

df.printSchema()

df.show()

# SQL
df.filter('Close < 500').show()

df.filter('Close < 500').select('Open').show()

# Python
df.filter(df['Close'] < 500).select('Volume').show()

df.filter((df['Close'] < 500) & (df['Open'] > 200)).show()

result = df.filter((df['Close'] < 500) & (df['Open'] > 200)).collect()

print(result)

row = result[0]
print(row.asDict())
