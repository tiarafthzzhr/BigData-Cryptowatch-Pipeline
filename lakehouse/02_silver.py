"""
02_silver.py — Bronze Delta Lake → Silver Delta Lake (Cleaning)
Tema: CryptoWatch (BTC, ETH, BNB)

Membaca dari Bronze layer, melakukan transformasi cleaning,
dan menyimpan ke Silver Delta layer.

Transformasi API (5 langkah):
  1. Cast timestamp string → TimestampType  → aktifkan Window Function
  2. Hapus duplikat (symbol, timestamp)      → consumer bisa double-flush
  3. Filter price_usd <= 0                   → harga koin tidak mungkin nol/negatif
  4. Handle null change_24h → isi 0.0        → cegah rusak agregasi volatilitas
  5. Ekstrak kolom 'jam' dari timestamp      → analisis temporal lebih mudah

Transformasi RSS (4 langkah):
  1. Cast timestamp → TimestampType
  2. Hapus duplikat (link)                   → RSS dipoll berulang, link = key unik
  3. Filter artikel tanpa judul              → tidak berguna untuk analisis topik
  4. Ekstrak kolom 'jam' dari timestamp

Di akhir script: demonstrasi Time Travel Delta Lake.
"""

import os
import platform
from pathlib import Path
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from pyspark.sql.functions import (
    col, to_timestamp, hour, when, lit, count as spark_count
)

# ── Path ──────────────────────────────────────────────────────────────
SCRIPT_DIR = Path(__file__).parent.resolve()

def to_uri(p):
    return str(p).replace("\\", "/")

_WIN_SPACES = platform.system() == "Windows" and " " in str(SCRIPT_DIR)
_BASE       = Path("C:/sparkdata") if _WIN_SPACES else SCRIPT_DIR / "lakehouse_data"

BRONZE_API_PATH = _BASE / "bronze" / "crypto_api"
BRONZE_RSS_PATH = _BASE / "bronze" / "crypto_rss"
SILVER_API_PATH = _BASE / "silver" / "crypto_api"
SILVER_RSS_PATH = _BASE / "silver" / "crypto_rss"

# ── SparkSession ──────────────────────────────────────────────────────
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


# ════════════════════════════════════════════════════════════════════
# CLEANING: API Data (Harga Koin Real-time)
# ════════════════════════════════════════════════════════════════════
print("\n  Cleaning data API...")

bronze_api = spark.read.format("delta").load(to_uri(BRONZE_API_PATH))
before_api = bronze_api.count()
print(f"  Sebelum cleaning : {before_api} records")
print(f"  Schema Bronze API:")
bronze_api.printSchema()

# Transformasi 1: Cast timestamp string → TimestampType
# Mengapa: Di ETS, timestamp disimpan sebagai string ISO ("2026-05-11T10:00:00").
#          String tidak bisa dipakai di Window Function dan fungsi temporal hour().
#          Casting ke TimestampType membuka akses ke semua fungsi temporal Spark.
api_clean = bronze_api.withColumn("timestamp", to_timestamp(col("timestamp")))

# Transformasi 2: Hapus duplikat (symbol, timestamp)
# Mengapa: Consumer melakukan flush buffer setiap 2 menit. Jika consumer direstart,
#          data yang sama bisa masuk dua kali. (symbol, timestamp) adalah kombinasi
#          unik per pengukuran harga.
before_dedup = api_clean.count()
api_clean = api_clean.dropDuplicates(["symbol", "timestamp"])
after_dedup = api_clean.count()
print(f"  [T2] Duplikat dihapus   : {before_dedup - after_dedup} baris")

# Transformasi 3: Filter harga tidak valid (price_usd <= 0)
# Mengapa: CoinGecko API mengembalikan 0 saat rate limit atau timeout.
#          Harga kripto tidak mungkin nol/negatif — data ini corrupt.
before_filter = api_clean.count()
api_clean = api_clean.filter(col("price_usd") > 0)
after_filter = api_clean.count()
print(f"  [T3] Harga invalid dihapus: {before_filter - after_filter} baris")

# Transformasi 4: Handle null pada change_24h → isi dengan 0.0
# Mengapa: Dipakai di analisis volatilitas. Null menyebabkan AVG() null untuk
#          seluruh kelompok jam, merusak visualisasi dashboard.
null_change = api_clean.filter(col("change_24h").isNull()).count()
api_clean = api_clean.withColumn(
    "change_24h",
    when(col("change_24h").isNull(), lit(0.0)).otherwise(col("change_24h"))
)
print(f"  [T4] Null change_24h diisi 0.0: {null_change} baris")

# Transformasi 5: Ekstrak kolom 'jam' dari timestamp
# Mengapa: Precomputed column menghindari kalkulasi ulang di setiap Gold query.
api_clean = api_clean.withColumn("jam", hour(col("timestamp")))

after_api = api_clean.count()
removed_api = before_api - after_api
pct_api = (removed_api / before_api * 100) if before_api > 0 else 0
print(f"\n  Sesudah cleaning : {after_api} records")
print(f"  Baris dihilangkan: {removed_api} ({pct_api:.1f}%) - duplikat + harga invalid")
api_clean.show(5, truncate=55)

api_clean.write.format("delta").mode("overwrite").save(to_uri(SILVER_API_PATH))
print(f"  Silver API tersimpan: {to_uri(SILVER_API_PATH)}")


# ════════════════════════════════════════════════════════════════════
# CLEANING: RSS Data (Berita Kripto)
# ════════════════════════════════════════════════════════════════════
print("\n  Cleaning data RSS...")

bronze_rss = spark.read.format("delta").load(to_uri(BRONZE_RSS_PATH))
before_rss = bronze_rss.count()
print(f"  Sebelum cleaning : {before_rss} records")

# Transformasi 1: Cast timestamp → TimestampType
rss_clean = bronze_rss.withColumn("timestamp", to_timestamp(col("timestamp")))

# Transformasi 2: Hapus duplikat berdasarkan link (URL artikel)
# Mengapa: RSS feed di-poll tiap 5 menit; artikel lama masih muncul di feed.
before_rss_dedup = rss_clean.count()
rss_clean = rss_clean.dropDuplicates(["link"])
print(f"  [T2] Duplikat RSS dihapus: {before_rss_dedup - rss_clean.count()} baris")

# Transformasi 3: Filter artikel tanpa judul
# Mengapa: Artikel tanpa judul adalah entry malformed dari RSS feed.
before_rss_filter = rss_clean.count()
rss_clean = rss_clean.filter(
    col("title").isNotNull() & (col("title") != "")
)
print(f"  [T3] Artikel tanpa judul: {before_rss_filter - rss_clean.count()} baris")

# Transformasi 4: Ekstrak kolom 'jam'
rss_clean = rss_clean.withColumn("jam", hour(col("timestamp")))

after_rss = rss_clean.count()
removed_rss = before_rss - after_rss
pct_rss = (removed_rss / before_rss * 100) if before_rss > 0 else 0
print(f"\n  Sesudah cleaning : {after_rss} records")
print(f"  Baris dihilangkan: {removed_rss} ({pct_rss:.1f}%)")
rss_clean.show(5, truncate=55)

rss_clean.write.format("delta").mode("overwrite").save(to_uri(SILVER_RSS_PATH))
print(f"  Silver RSS tersimpan: {to_uri(SILVER_RSS_PATH)}")


# ════════════════════════════════════════════════════════════════════
# DEMONSTRASI TIME TRAVEL — Delta Lake
# ════════════════════════════════════════════════════════════════════
print("\n" + "=" * 65)
print("  TIME TRAVEL DEMONSTRATION - Delta Lake")
print("=" * 65)

delta_table = DeltaTable.forPath(spark, to_uri(SILVER_API_PATH))

# Langkah 1: Tampilkan history tabel
print("\n  History tabel Silver API:")
delta_table.history().select("version", "timestamp", "operation").show(truncate=False)

# Langkah 2: Update — change_24h = 0.0 → -999.0 (simulasi koreksi data)
print("  UPDATE: change_24h = 0.0  ->  -999.0 (simulasi)")
delta_table.update(
    condition="change_24h = 0.0",
    set={"change_24h": lit(-999.0)}
)
print("  Update selesai.")

# Langkah 3: History setelah update
print("\n  History setelah UPDATE:")
delta_table.history().select("version", "timestamp", "operation").show(truncate=False)

# Langkah 4: Bandingkan versi sekarang vs versi 0
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

# Langkah 5: Restore ke versi 0 untuk Gold layer
print("\n  Restore ke versi 0 untuk Gold layer...")
clean_v0 = spark.read.format("delta").option("versionAsOf", 0).load(to_uri(SILVER_API_PATH))
clean_v0.write.format("delta").mode("overwrite").save(to_uri(SILVER_API_PATH))
print("  Silver API dikembalikan ke data bersih (versi 0).")

print("\n" + "=" * 65)
print("  Silver Layer selesai! Lanjutkan ke 03_gold.py")
print("=" * 65)

spark.stop()
