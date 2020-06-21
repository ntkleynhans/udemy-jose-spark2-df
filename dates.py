from pyspark.sql import SparkSession
from pyspark.sql.functions import (dayofmonth,hour,dayofyear,
            month,year,weekofyear,format_number,date_format)

spark = SparkSession.builder.appName('dates').getOrCreate()

df = spark.read.csv('./data/appl_stock.csv',inferSchema=True,header=True)

df.show()

df.printSchema()

df.select(['Date']).show()

df.select(dayofmonth(df['Date'])).show()

df.select(hour(df['Date'])).show()

df.select(year(df['Date'])).show()

ndf = df.withColumn('Year', year(df['Date']))
ndf.groupBy('Year').mean().select(['Year', 'avg(Close)']).show()

result = ndf.groupBy('Year').mean().select(['Year', 'avg(Close)'])

result.withColumnRenamed('avg(Close)', 'Average Closing Price').select(['Year', format_number('Average Closing Price', 2)]).show()