from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import (
    ArrayType,
    StringType,
    IntegerType,
    BooleanType,
    TimestampType,
)


class TweetsCleaner:

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def clean_tweets(self, df: DataFrame) -> DataFrame:
        return (
            df.withColumn(
                "hashtags", from_json(col("hashtags"), ArrayType(StringType()))
            )
            .withColumn("user_created", col("user_created").cast(TimestampType()))
            .withColumn(
                "user_followers", col("user_followers").try_cast(IntegerType())
            )  # used try_cast due to ValueError
            .withColumn("user_friends", col("user_friends").cast(IntegerType()))
            .withColumn("user_favourites", col("user_favourites").cast(IntegerType()))
            .withColumn("date", col("date").cast(TimestampType()))
            .withColumn("user_verified", col("user_verified").cast(BooleanType()))
            .withColumn("verified", col("verified").cast(BooleanType()))
            .withColumn("is_retweet", col("is_retweet").cast(BooleanType()))
        )

    def hashtag_normalization(self, df: DataFrame) -> DataFrame:
        hashtag_lower_df = df.withColumn("hashtag_lower", lower(col("hashtag")))
        hashtag_mapped_df = hashtag_lower_df.withColumn(
            "hashtags",
            when(
                col("hashtag_lower").isin(
                    "covid",
                    "covid19",
                    "covid_19",
                    "covid__19",
                    "covıd19",
                    "covid2019",
                    "covidー19",
                    "corona",
                    "coronavirus",
                    "novelcoronavirus",
                    "chinavirus",
                    "chinesevirus",
                    "wuhanvirus",
                    "sarscov2",
                    "sars_cov_2",
                    "pandemic",
                    "plandemic",
                    "scamdemic",
                    "longcovid",
                    "asymptomatic",
                    "herdimmunity",
                    "flattenthecurve",
                    "stopthespread",
                    "lockdown",
                    "lockdowns",
                    "lockdown2020",
                    "lockdownextension",
                    "lockdownsa",
                    "quarantine",
                    "quarantinelife",
                    "stayhome",
                    "stayathome",
                    "stayhomestaysafe",
                    "staysafe",
                    "testing",
                    "contacttracing",
                    "covidtaskforce",
                    "covidvaccine",
                    "vaccine",
                    "vaccines",
                    "russianvaccine",
                    "moderna",
                    "remdesivir",
                    "hydroxychloroquine",
                    "hcq",
                    "mask",
                    "masks",
                    "maskup",
                    "wearamask",
                    "wearadamnmask",
                    "facemask",
                    "facemasks",
                    "facecoverings",
                    "worldmaskweek",
                ),
                "covid19",
            )
            .when(
                col("hashtag_lower").isin(
                    "trump",
                    "donaldtrump",
                    "trump2020",
                    "trumpvirus",
                    "trumpfailure",
                    "trumpfailedamerica",
                    "trumpliesamerican...",
                    "trumphasnoplan",
                    "trumpisnotwell",
                    "trumpisacompletef...",
                    "trumpviruscatastr...",
                ),
                "trump",
            )
            .when(
                col("hashtag_lower").isin(
                    "grammys", "grammy", "grammynominations", "grammys2020"
                ),
                "grammys",
            )
            .when(
                col("hashtag_lower").isin(
                    "india",
                    "indian",
                    "indiafightscorona",
                    "indiafightscovid19",
                    "odisha",
                    "delhi",
                    "mumbai",
                    "kerala",
                    "tamilnadu",
                    "punjab",
                    "gujarat",
                    "jharkhand",
                    "uttarpradesh",
                    "maharashtra",
                ),
                "india",
            )
            .when(
                col("hashtag_lower").isin(
                    "china",
                    "beijing",
                    "hongkong",
                    "wuhan",
                    "chinavirus",
                    "chinesevirus",
                    "chinese",
                ),
                "china",
            )
            .when(
                col("hashtag_lower").isin(
                    "usa",
                    "us",
                    "america",
                    "american",
                    "americans",
                    "unitedstates",
                    "nyc",
                    "california",
                    "texas",
                    "florida",
                    "michigan",
                    "newyork",
                    "washington",
                ),
                "usa",
            )
            .when(
                col("hashtag_lower").isin(
                    "russia", "russian", "moscow", "russianvaccine", "russiareport"
                ),
                "russia",
            )
            .otherwise(
                col("hashtag_lower")
            ),  # if not match, then leave original value
        )
        return hashtag_mapped_df
