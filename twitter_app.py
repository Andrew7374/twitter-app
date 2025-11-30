"""

Plan projektu:

0. Rozpoznanie danych - co to jest, jaki jest schemat, ile ich jest, które kolmuny są pozorne, czyli np. są stringiem,
a np przechowują dane liczbowe albo daty?
1. Ładowanie danych - Loader
2. Czyszczenie danych - CLeaner
3. Analiza i przeszukiwanie - Analyzer

"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from loaders.loader_csv import CsvLoader
from cleaners.cleaner import TweetsCleaner
from analyzers.tweets_analyzer import TweetsAnalyzer
from analyzers.tweets_search import TweetsSearch

if __name__ == "__main__":
    spark = SparkSession.builder.appName("twitter_app").getOrCreate()

    loader = CsvLoader(spark)
    cleaner = TweetsCleaner(spark)
    analyzer = TweetsAnalyzer(spark)
    search = TweetsSearch(spark)

    # LOAD & CLEAN
    tweets_df = loader.load_tweets().cache()
    tweets_cleaned_df = cleaner.clean_tweets(tweets_df)

    # ANALYZE
    # Popularność hashtagów oryginalna forma
    hashtags_stats_df = analyzer.calculate_hashtags(tweets_cleaned_df)
    print("Popularnosc hashtagow przed mappingiem")
    hashtags_stats_df.sort(col("count").desc()).show(10, truncate=False)

    # Popularność hashtagów po normalizacji najpopularniejszych
    hashtags_mapped_stats_df = analyzer.calculate_hashtags_norm(tweets_cleaned_df)
    print("Popularnossc hashtagow po mapowaniu")
    hashtags_mapped_stats_df.sort(col("count").desc()).show(10, truncate=False)

    # Statystyka z jakich wersji Twittera pochodzą tweety wspominające Trumpa wysłane na terenie USA
    trump_tweets_df = tweets_cleaned_df.transform(
        lambda df: search.search_by_keyword(df, "Trump")
    ).transform(lambda df: search.search_by_location(df, "United States"))
    source_count_df = analyzer.calculate_source_count(trump_tweets_df)
    source_count_df.show()
