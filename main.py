import pyspark
import sys
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
import pyspark.sql.functions as F
import argparse

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--path_input", help="path of the input csv file.")
    parser.add_argument("--path_output", help="path where the parquet output file will be saved.")
    args = parser.parse_args()
    if args.path_input and args.path_output:
        path_input = args.path_input
        path_output = args.path_output
    else:
        print("Application needs 2 arguments , type -h to see Help")



    df = spark.read.option("header","true").option("sep",";").csv(path_input)

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