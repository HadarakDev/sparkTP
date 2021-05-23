import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
import pyspark.sql.functions as F

spark = SparkSession.builder.getOrCreate()


df = spark.read.option("header","true").option("sep",";").csv("persons.csv")

df.show()

# agg_by_class = (df
#     .agg(
#         F.avg(df.Age.cast("int")).alias("avg_age"),
#         F.min(df.Age.cast("int")).alias("min_age"),
#         F.max(df.Age.cast("int")).alias("max_age"),
#         F.count(()))
#     )
# )
sum_cond = lambda cond: F.sum(F.when(cond, 1).otherwise(0))

df2 = df.agg(
    sum_cond(F.col("Age").between(0, 17)).alias('under_age'),
    sum_cond(F.col("Age") > 20).alias('> 20')
    )
df2.show()
# agg_by_class.show()