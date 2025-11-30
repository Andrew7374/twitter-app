from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *


class TweetsSearch:
    ### Mappings
    TEXT_COLUMN = "text"
    LOCATION_COLUMN = "user_location"

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def search_by_keyword(self, df: DataFrame, keyword: str) -> DataFrame:
        return df.filter(col(TweetsSearch.TEXT_COLUMN).contains(keyword))

    def search_by_keywords(self, df: DataFrame, keywords: str) -> DataFrame:
        return (
            df.withColumn(
                "keywordsResult",
                array_intersect(
                    split(col(TweetsSearch.TEXT_COLUMN), " "), split(lit(keywords), " ")
                ),
            )
            .filter(
                ~(col("keywordsResult").isNull() | (size(col("keywordsResult")) == 0))
            )
            .drop("keywordsResult")
        )

    def search_by_location(self, df: DataFrame, location: str) -> DataFrame:
        return df.filter(col(TweetsSearch.LOCATION_COLUMN) == location)
