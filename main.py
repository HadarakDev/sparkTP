import pyspark
import sys
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
import pyspark.sql.functions as F
import argparse
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType


def local_match(df, path_parquet):
    df_local = df.withColumn("local_match", F.when(F.split(df.match, "-").getItem(0) == df.adversaire, True).otherwise(False))
    
    df_local = ((df_local.filter(df_local.adversaire != "France") and  df_local.filter(df_local.match.contains("France")))
        .agg(
            F.avg(df_local.score_france.cast("int")).alias("but_france"),
            F.avg(df_local.score_adversaire.cast("int")).alias("but_adversaire"),
            F.count(df_local.match).alias("nombre_match"),
            F.avg(df_local.local_match.cast("int")).alias("match_local"),
            F.max(df_local.penalty_france.cast("int")).alias("penalty_max"),
            F.count(df_local.competition.contains("Monde")).alias("count_coupe_du_monde"),
            (F.count(df_local.penalty_france.cast("int")) - F.count(df_local.penalty_adversaire.cast("int"))).alias("penalty_2"),
            )
        )
    df_local.show()
    # df_local.write.parquet(path_parquet)


if __name__ == "__main__":
    spark = SparkSession.builder.master("local").appName("FootballApp").getOrCreate()
    logger = SparkContext._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel( logger.Level.ERROR )
    logger.LogManager.getLogger("akka").setLevel( logger.Level.ERROR )
    
    # parser = argparse.ArgumentParser()
    # parser.add_argument("--path_input", help="path of the input csv file.")
    # parser.add_argument("--path_output", help="path where the parquet output file will be saved.")
    # args = parser.parse_args()
    # if args.path_input and args.path_output:
    #     path_input = args.path_input
    #     path_output = args.path_output
    # else:
    #     print("Application needs 2 arguments , type -h to see Help")

    if len(sys.argv) < 3:
        print("Application needs 2 arguments")
        exit()
    df = spark.read.option("header","true").option("sep",",").csv(sys.argv[1])

    df = df.withColumnRenamed("X4", "match")
    df = df.withColumnRenamed("X6", "competition")
    df = df.select("match", "competition", "adversaire", "score_france", "score_adversaire", "penalty_france", "penalty_adversaire", "date")
    df = df.replace("NA", "0").replace(int("0"), 0)
    df = df.filter(df["date"] > F.lit("1980-01-01"))
    df.show()
    match_udf = udf(lambda x, y: local_match(x, y))
    local_match(df, sys.argv[2])
    match_udf(df, sys.argv[2])
