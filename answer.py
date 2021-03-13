from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession .builder.appName("Data Frame Example").config("spark.some.config.option", "some-value").getOrCreate()
jul = spark.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter", "\t").load("nasa_19950701.tsv")
jul.show(3)

aug = spark.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter", "\t").load("nasa_19950801.tsv")
aug.show(3)

join_key = [(col('a.host') == col('b.host'))]

both_days = jul.alias('a').join(aug.alias('b'),join_key, "inner").select([col('a.'+x) for x in jul.columns])


both_days.show(200)

both_days.select("host").distinct().coalesce(1).write.format("text").option("header", "false").mode("overwrite").save("same_hosts")
