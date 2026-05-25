# 02_silver.py
# Cleaning data dari Bronze layer dan simpan ke Silver
# Ada demonstrasi Time Travel Delta Lake di bagian bawah

import os
import platform
from pathlib import Path
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from pyspark.sql.functions import (
    col, to_timestamp, hour, when, lit, count as spark_count
)

SCRIPT_DIR = Path(__file__).parent.resolve()

def to_uri(p):
    return str(p).replace("\\", "/")

_WIN_SPACES = platform.system() == "Windows" and " " in str(SCRIPT_DIR)
_BASE       = Path("C:/sparkdata") if _WIN_SPACES else SCRIPT_DIR / "lakehouse_data"

BRONZE_API_PATH = _BASE / "bronze" / "crypto_api"
BRONZE_RSS_PATH = _BASE / "bronze" / "crypto_rss"
SILVER_API_PATH = _BASE / "silver" / "crypto_api"
SILVER_RSS_PATH = _BASE / "silver" / "crypto_rss"

builder = (
    SparkSession.builder
    .appName("Silver-CryptoWatch")
    .master("local[*]")
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.driver.memory", "2g")
    .config("spark.ui.enabled", "false")
)

spark = configure_spark_with_delta_pip(
    builder,
    extra_packages=["io.delta:delta-spark_2.12:3.3.2"]
).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("=" * 65)
print("  SILVER LAYER - CryptoWatch Lakehouse")
print("=" * 65)


print("\n  Cleaning data API...")

bronze_api = spark.read.format("delta").load(to_uri(BRONZE_API_PATH))
before_api = bronze_api.count()
print(f"  Sebelum cleaning : {before_api} records")
print(f"  Schema Bronze API:")
bronze_api.printSchema()

# T1: timestamp masih string, perlu dicast biar bisa dipakai hour() dan window function
api_clean = bronze_api.withColumn("timestamp", to_timestamp(col("timestamp")))

# T2: consumer kadang kirim data dua kali kalau direstart, hapus duplikat
before_dedup = api_clean.count()
api_clean = api_clean.dropDuplicates(["symbol", "timestamp"])
after_dedup = api_clean.count()
print(f"  [T2] Duplikat dihapus   : {before_dedup - after_dedup} baris")

# T3: API kadang return price=0 saat rate limit, data ini tidak valid
before_filter = api_clean.count()
api_clean = api_clean.filter(col("price_usd") > 0)
after_filter = api_clean.count()
print(f"  [T3] Harga invalid dihapus: {before_filter - after_filter} baris")

# T4: change_24h null bikin avg() jadi null di Gold, isi 0.0 supaya aman
null_change = api_clean.filter(col("change_24h").isNull()).count()
api_clean = api_clean.withColumn(
    "change_24h",
    when(col("change_24h").isNull(), lit(0.0)).otherwise(col("change_24h"))
)
print(f"  [T4] Null change_24h diisi 0.0: {null_change} baris")

# T5: precompute kolom jam biar tidak perlu hitung ulang di setiap query Gold
api_clean = api_clean.withColumn("jam", hour(col("timestamp")))

after_api = api_clean.count()
removed_api = before_api - after_api
pct_api = (removed_api / before_api * 100) if before_api > 0 else 0
print(f"\n  Sesudah cleaning : {after_api} records")
print(f"  Baris dihilangkan: {removed_api} ({pct_api:.1f}%) - duplikat + harga invalid")
api_clean.show(5, truncate=55)

api_clean.write.format("delta").mode("overwrite").save(to_uri(SILVER_API_PATH))
print(f"  Silver API tersimpan: {to_uri(SILVER_API_PATH)}")


print("\n  Cleaning data RSS...")

bronze_rss = spark.read.format("delta").load(to_uri(BRONZE_RSS_PATH))
before_rss = bronze_rss.count()
print(f"  Sebelum cleaning : {before_rss} records")

# T1: cast timestamp
rss_clean = bronze_rss.withColumn("timestamp", to_timestamp(col("timestamp")))

# T2: RSS di-poll tiap 5 menit jadi artikel lama sering muncul lagi, hapus pakai link sebagai key unik
before_rss_dedup = rss_clean.count()
rss_clean = rss_clean.dropDuplicates(["link"])
print(f"  [T2] Duplikat RSS dihapus: {before_rss_dedup - rss_clean.count()} baris")

# T3: hapus artikel tanpa judul (entry malformed dari feed)
before_rss_filter = rss_clean.count()
rss_clean = rss_clean.filter(
    col("title").isNotNull() & (col("title") != "")
)
print(f"  [T3] Artikel tanpa judul: {before_rss_filter - rss_clean.count()} baris")

# T4: ekstrak jam untuk join dengan API di Gold
rss_clean = rss_clean.withColumn("jam", hour(col("timestamp")))

after_rss = rss_clean.count()
removed_rss = before_rss - after_rss
pct_rss = (removed_rss / before_rss * 100) if before_rss > 0 else 0
print(f"\n  Sesudah cleaning : {after_rss} records")
print(f"  Baris dihilangkan: {removed_rss} ({pct_rss:.1f}%)")
rss_clean.show(5, truncate=55)

rss_clean.write.format("delta").mode("overwrite").save(to_uri(SILVER_RSS_PATH))
print(f"  Silver RSS tersimpan: {to_uri(SILVER_RSS_PATH)}")


print("\n" + "=" * 65)
print("  TIME TRAVEL DEMONSTRATION - Delta Lake")
print("=" * 65)

delta_table = DeltaTable.forPath(spark, to_uri(SILVER_API_PATH))

# cek history sebelum diapa-apain
print("\n  History tabel Silver API:")
delta_table.history().select("version", "timestamp", "operation").show(truncate=False)

# simulasi koreksi data: ubah change_24h=0 jadi -999 supaya kelihatan bedanya
print("  UPDATE: change_24h = 0.0  ->  -999.0 (simulasi)")
delta_table.update(
    condition="change_24h = 0.0",
    set={"change_24h": lit(-999.0)}
)
print("  Update selesai.")

# history sekarang harusnya ada 2 versi
print("\n  History setelah UPDATE:")
delta_table.history().select("version", "timestamp", "operation").show(truncate=False)

# bandingkan: versi terbaru punya sentinel -999, versi 0 belum
print("\n=== Data SEKARANG (setelah update) ===")
spark.read.format("delta").load(to_uri(SILVER_API_PATH)) \
    .groupBy("symbol") \
    .agg(
        spark_count(when(col("change_24h") == -999.0, True)).alias("sentinel_count"),
        spark_count("*").alias("total")
    ).show()

print("=== Data VERSI 0 (sebelum update - Time Travel) ===")
spark.read.format("delta").option("versionAsOf", 0).load(to_uri(SILVER_API_PATH)) \
    .groupBy("symbol") \
    .agg(
        spark_count(when(col("change_24h") == 0.0, True)).alias("change_0_count"),
        spark_count("*").alias("total")
    ).show()

print("  Time Travel BERHASIL! Versi lama masih bisa diakses.")

# kembalikan ke versi bersih sebelum Gold layer baca
print("\n  Restore ke versi 0 untuk Gold layer...")
clean_v0 = spark.read.format("delta").option("versionAsOf", 0).load(to_uri(SILVER_API_PATH))
clean_v0.write.format("delta").mode("overwrite").save(to_uri(SILVER_API_PATH))
print("  Silver API dikembalikan ke data bersih (versi 0).")

print("\n" + "=" * 65)
print("  SCHEMA EVOLUTION DEMONSTRATION - Delta Lake")
print("=" * 65)

# baca Silver yang sudah di-restore, lalu tambah kolom kategorisasi harga
silver_for_evolution = spark.read.format("delta").load(to_uri(SILVER_API_PATH))

# kolom baru: price_tier berdasarkan threshold harga koin
silver_evolved = silver_for_evolution.withColumn(
    "price_tier",
    when(col("price_usd") > 50000, lit("high"))
    .when(col("price_usd") > 1000, lit("mid"))
    .otherwise(lit("low"))
)

# tanpa mergeSchema=true, Delta akan throw AnalysisException karena skema berbeda
# dengan mergeSchema=true, kolom baru diterima dan skema Delta diperbarui
silver_evolved.write \
    .format("delta") \
    .option("mergeSchema", "true") \
    .mode("overwrite") \
    .save(to_uri(SILVER_API_PATH))

print("  Kolom 'price_tier' berhasil ditambahkan ke Silver API (mergeSchema=true)!")
print("  Schema Silver API sekarang:")
spark.read.format("delta").load(to_uri(SILVER_API_PATH)).printSchema()
print("  Distribusi price_tier:")
spark.read.format("delta").load(to_uri(SILVER_API_PATH)) \
    .groupBy("price_tier").count().orderBy("price_tier").show()

print("\n" + "=" * 65)
print("  Silver Layer selesai! Lanjutkan ke 03_gold.py")
print("=" * 65)

spark.stop()
