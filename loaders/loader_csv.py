from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit


class CsvLoader:
    def __init__(self, spark):
        self.spark = spark

    def load_raw_file(self, file_path: str, file_category: str) -> DataFrame:
        return (
            self.spark.read.option("header", True)
            .csv(file_path)
            .withColumn("category", lit(file_category))
            .na.drop()
        )

    def load_tweets(self) -> DataFrame:
        covid_df = self.load_raw_file("source/covid19_tweets.csv", "covid")
        grammy_df = self.load_raw_file("source/GRAMMYs_tweets.csv", "grammy")
        financial_df = self.load_raw_file("source/financial.csv", "financial")

        return covid_df.unionByName(grammy_df, allowMissingColumns=True).unionByName(
            financial_df, allowMissingColumns=True
        )
