from pyspark.sql import SparkSession
from pyspark.sql.functions import stddev, format_number, avg, min, max, year, month
from pyspark.sql.types import FloatType

spark = SparkSession.builder.appName('Project').getOrCreate()

df = spark.read.csv('./data/walmart_stock.csv',header=True,inferSchema=True)

print(df.columns)

df.printSchema()

df.show(5)

print(df.head(5))

df.describe().show()

ddf = df.describe()

for col in ['Open', 'High', 'Low', 'Close', 'Volume', 'Adj Close']:
    dff = ddf.withColumn(col, ddf[col].cast(FloatType()))
    dff = dff.withColumn(col, format_number(col, 2).alias(col))    
ddf.show()

df.withColumn('HR Ratio', df['High'] / df['Volume']).select('HR Ratio').show()

df.orderBy(df['High'].desc()).head(1)[0][0]

df.select(avg('Close')).show()

df.select([max('Volume'), min('Volume')]).show()

print(df.filter(df['Close'] < 60).count())

print(df.select(df['High'] > 80).count() / df.count() * 100.0)

print(df.corr('High', 'Volume'))

df.select([year('Date').alias('Year'), 'High']).groupBy('Year').max().select(['Year', 'max(High)']).show()

df.select([month('Date').alias('Month'), 'Close']).groupBy('Month').mean().select(['Month', 'avg(Close)']).orderBy('Month').show()
