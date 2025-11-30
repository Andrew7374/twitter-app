from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from cleaners.cleaner import TweetsCleaner


class TweetsAnalyzer:
    ### Mappings
    HASHTAG_COLUMN = "hashtags"
    IS_RETWEET_COLUMN = "is_retweet"
    SOURCE_COLUMN = "source"
    USER_NAME_COLUMN = "user_name"
    USER_FOLLOWERS_COLUMN = "user_followers"
    USER_LOCATION_COLUMN = "user_location"

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def calculate_hashtags(self, df: DataFrame) -> DataFrame:
        return (
            df.withColumn(
                TweetsAnalyzer.HASHTAG_COLUMN,
                explode_outer(col(TweetsAnalyzer.HASHTAG_COLUMN)),
            )
            .groupBy(TweetsAnalyzer.HASHTAG_COLUMN)
            .count()
        )

    def calculate_hashtags_norm(self, df: DataFrame) -> DataFrame:
        cleaner = TweetsCleaner(self.spark)
        hashtags_exploded_df = df.withColumn(
            "hashtag", explode_outer(col(TweetsAnalyzer.HASHTAG_COLUMN))
        )
        hashtags_mapped_df = cleaner.hashtag_normalization(hashtags_exploded_df)
        hashtags_mapped_stats_df = hashtags_mapped_df.groupBy(
            TweetsAnalyzer.HASHTAG_COLUMN
        ).count()
        return hashtags_mapped_stats_df

    def calculate_is_retweet_count(self, df: DataFrame) -> DataFrame:
        return df.groupBy(TweetsAnalyzer.IS_RETWEET_COLUMN).count()

    def calculate_source_count(self, df: DataFrame) -> DataFrame:
        return df.groupBy(TweetsAnalyzer.SOURCE_COLUMN).count()

    def calculate_avg_user_per_followers_per_location(self, df: DataFrame) -> DataFrame:
        return (
            (
                df.select(
                    TweetsAnalyzer.USER_NAME_COLUMN,
                    TweetsAnalyzer.USER_FOLLOWERS_COLUMN,
                    TweetsAnalyzer.USER_LOCATION_COLUMN,
                )
                .filter(col(TweetsAnalyzer.USER_NAME_COLUMN).isNotNull())
                .filter(col(TweetsAnalyzer.USER_LOCATION_COLUMN).isNotNull())
            )
            .dropDuplicates([TweetsAnalyzer.USER_NAME_COLUMN])  # list must be passed
            .groupBy(TweetsAnalyzer.USER_LOCATION_COLUMN)
            .avg(TweetsAnalyzer.USER_FOLLOWERS_COLUMN)
            .alias("avg")
        )
