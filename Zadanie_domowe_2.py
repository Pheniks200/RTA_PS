from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, window, avg, round as _round, count, desc

spark = (
    SparkSession.builder
    .appName("zadanko_2")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

df = spark.read.json("transactions_10k.jsonl")

df = df.withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))

gdansk_lowest_avg = (
    df.filter(col("store") == "Gdańsk")
    .groupBy(window("timestamp", "1 hour"))
    .agg(_round(avg("amount"), 2).alias("srednia_PLN"))
    .orderBy("srednia_PLN") 
)

print("Godzina z najniższą średnią kwotą transakcji dla sklepu Gdańsk:")
gdansk_lowest_avg.show(1, truncate=False)

cat_0900_0930 = (
    df.groupBy(window("timestamp", "30 minutes"), "category")
    .agg(count("tx_id").alias("liczba_tx"))
    .filter(col("window.start").cast("string").like("%09:00:00%"))
    .orderBy("category")
)

print("Liczba transakcji per kategoria w oknie 09:00–09:30:")

cat_0900_0930.show(truncate=False)

transaction_peak = (
    df.groupBy(window("timestamp", "15 minutes"))
    .agg(count("tx_id").alias("liczba_tx"))
    .orderBy(desc("liczba_tx"))
)

print("Cwiercgodziny z najwieksza liczba transakcji:")
transaction_peak.show(1, truncate=False)

spark.stop()
