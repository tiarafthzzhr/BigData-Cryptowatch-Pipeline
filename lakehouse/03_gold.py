# 03_gold.py
# Agregasi Silver → 4 tabel Gold untuk CryptoWatch (BTC/ETH/BNB)
# 2 tabel reproduksi ETS + 2 tabel enhanced (butuh data bersih dari Silver)

import os
import platform
from pathlib import Path
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from pyspark.sql.functions import (
    col, avg, count, max as spark_max, min as spark_min,
    stddev, abs as spark_abs, round as spark_round,
    when, lit
)

SCRIPT_DIR = Path(__file__).parent.resolve()

def to_uri(p):
    return str(p).replace("\\", "/")

_WIN_SPACES = platform.system() == "Windows" and " " in str(SCRIPT_DIR)
_BASE       = Path("C:/sparkdata") if _WIN_SPACES else SCRIPT_DIR / "lakehouse_data"

SILVER_API_PATH      = _BASE / "silver" / "crypto_api"
SILVER_RSS_PATH      = _BASE / "silver" / "crypto_rss"
GOLD_STATS_PATH      = _BASE / "gold" / "crypto_stats"
GOLD_VOLATILITY_PATH = _BASE / "gold" / "crypto_hourly_volatility"
GOLD_SPIKE_PATH      = _BASE / "gold" / "crypto_spike_alerts"
GOLD_JOIN_PATH       = _BASE / "gold" / "crypto_news_price_join"

builder = (
    SparkSession.builder
    .appName("Gold-CryptoWatch")
    .master("local[*]")
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.driver.memory", "2g")
    .config("spark.ui.enabled", "false")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.driver.host", "127.0.0.1")
)

spark = configure_spark_with_delta_pip(
    builder,
    extra_packages=["io.delta:delta-spark_2.12:3.3.2"]
).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

silver_api = spark.read.format("delta").load(to_uri(SILVER_API_PATH))
silver_rss = spark.read.format("delta").load(to_uri(SILVER_RSS_PATH))

print("=" * 65)
print("  GOLD LAYER - CryptoWatch Lakehouse")
print(f"  Silver API : {silver_api.count()} records")
print(f"  Silver RSS : {silver_rss.count()} records")
print("=" * 65)


print("\n  GOLD 1 - Statistik Harga Per Simbol (Reproduksi ETS Analisis 1)")
print("  Perbedaan vs ETS: data sudah dedup -> stddev/avg lebih akurat")
print("-" * 65)

gold_stats = (
    silver_api
    .groupBy("symbol")
    .agg(
        spark_round(avg("price_usd"),       2).alias("avg_price_usd"),
        spark_round(spark_max("price_usd"), 2).alias("max_price_usd"),
        spark_round(spark_min("price_usd"), 2).alias("min_price_usd"),
        spark_round(stddev("price_usd"),    2).alias("stddev_price"),
        spark_round(avg("price_idr"),       0).alias("avg_price_idr"),
        count("*").alias("total_records")
    )
    .orderBy(col("avg_price_usd").desc())
)

gold_stats.show()
gold_stats.write.format("delta").mode("overwrite").save(to_uri(GOLD_STATS_PATH))
print(f"  Tersimpan: {to_uri(GOLD_STATS_PATH)}")


print("\n  GOLD 2 - Volatilitas Per Jam (Reproduksi ETS Analisis 2)")
print("  Perbedaan vs ETS: timestamp sudah TimestampType -> hour() presisi")
print("-" * 65)

gold_volatility = (
    silver_api
    .groupBy("jam")
    .agg(
        spark_round(avg(spark_abs(col("change_24h"))), 4).alias("avg_volatility"),
        spark_round(spark_max(spark_abs(col("change_24h"))), 4).alias("max_volatility"),
        count("*").alias("jumlah_data")
    )
    .orderBy("jam")
)

gold_volatility.show(24)
gold_volatility.write.format("delta").mode("overwrite").save(to_uri(GOLD_VOLATILITY_PATH))
print(f"  Tersimpan: {to_uri(GOLD_VOLATILITY_PATH)}")


print("\n  GOLD 3 - Deteksi Spike Anomali Harga (Enhanced - BARU)")
print("  Metode: Z-Score per simbol (|z| > 2 = anomali)")
print("  Tidak bisa di ETS: duplikat dan price=0 merusak mean/stddev")
print("-" * 65)

# hitung mean dan stddev dulu per simbol, baru join untuk z-score
baseline = (
    silver_api
    .groupBy("symbol")
    .agg(
        avg("price_usd").alias("mean_price"),
        stddev("price_usd").alias("std_price")
    )
)

print("\n  Baseline statistik per simbol:")
baseline.show()

spike_df = (
    silver_api
    .join(baseline, "symbol")
    .withColumn(
        "z_score",
        (col("price_usd") - col("mean_price")) / col("std_price")
    )
    .filter(spark_abs(col("z_score")) > 2)
    .select(
        "symbol", "timestamp", "jam",
        spark_round(col("price_usd"),  2).alias("price_usd"),
        spark_round(col("mean_price"), 2).alias("mean_price"),
        spark_round(col("std_price"),  2).alias("std_price"),
        spark_round(col("z_score"),    3).alias("z_score"),
        col("change_24h")
    )
    .orderBy(spark_abs(col("z_score")).desc())
)

spike_count = spike_df.count()
print(f"\n  Ditemukan {spike_count} kejadian anomali (|z-score| > 2):")
spike_df.show(20, truncate=40)

spike_df.write.format("delta").mode("overwrite").save(to_uri(GOLD_SPIKE_PATH))
print(f"  Tersimpan: {to_uri(GOLD_SPIKE_PATH)}")


print("\n  GOLD 4 - Berita di Sekitar Lonjakan Harga (Enhanced - BARU)")
print("  Join Silver RSS + Silver API berdasarkan jam")
print("  Tidak bisa di ETS: API & RSS terpisah, timestamp string")
print("-" * 65)

try:
    # ringkas spike ke level jam supaya bisa di-join sama data berita
    spike_by_hour = (
        spike_df
        .groupBy("symbol", "jam")
        .agg(
            count("*").alias("jumlah_spike"),
            spark_round(avg(spark_abs(col("z_score"))), 3).alias("avg_zscore")
        )
    )

    # hitung jumlah berita per jam dari RSS
    news_by_hour = (
        silver_rss
        .groupBy("jam")
        .agg(
            count("*").alias("jumlah_berita"),
            count("source").alias("jumlah_sumber")
        )
        .withColumnRenamed("jam", "jam_berita")
    )

    news_join = (
        spike_by_hour
        .join(news_by_hour, col("jam") == col("jam_berita"), "left")
        .select(
            "symbol",
            col("jam").alias("jam_spike"),
            "jumlah_spike",
            "avg_zscore",
            when(col("jumlah_berita").isNull(), 0)
                .otherwise(col("jumlah_berita")).alias("jumlah_berita_sejam"),
            when(col("jumlah_sumber").isNull(), 0)
                .otherwise(col("jumlah_sumber")).alias("jumlah_sumber")
        )
        .orderBy("symbol", "jam_spike")
    )

    print("\n  Berita vs spike per jam:")
    news_join.show(truncate=False)

    # Ringkasan korelasi
    news_join.createOrReplaceTempView("news_spike")
    print("  Korelasi berita vs spike per simbol:")
    spark.sql("""
        SELECT symbol,
               SUM(jumlah_spike) AS total_spike,
               SUM(jumlah_berita_sejam) AS total_berita_pada_jam_spike,
               ROUND(AVG(jumlah_berita_sejam), 1) AS rata_berita_per_jam_spike
        FROM news_spike
        GROUP BY symbol
        ORDER BY total_spike DESC
    """).show()

    news_join.write.format("delta").mode("overwrite").save(to_uri(GOLD_JOIN_PATH))
    print(f"  Tersimpan: {to_uri(GOLD_JOIN_PATH)}")

except Exception as e:
    print(f"  Gold 4 error: {e}")


print("\n" + "=" * 65)
print("  GOLD LAYER SELESAI")
print("=" * 65)
print("""
  Tabel                          Tipe        Keterangan
  ---------------------------------------------------------------
  gold/crypto_stats              Reproduksi  avg/min/max/stddev
  gold/crypto_hourly_volatility  Reproduksi  volatilitas per jam
  gold/crypto_spike_alerts       Enhanced    anomali z-score > 2
  gold/crypto_news_price_join    Enhanced    cross-join RSS + API
  ---------------------------------------------------------------
  Semua tersimpan dalam format Delta Lake (ACID, versioned).
""")

spark.stop()
