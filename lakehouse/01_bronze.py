"""
01_bronze.py — HDFS JSON → Bronze Delta Lake
Tema: CryptoWatch (BTC, ETH, BNB)

Membaca file JSON mentah dari HDFS, menambahkan metadata kolom,
dan menyimpan ke format Delta Lake sebagai Bronze layer.

Skema data masuk:
  API : symbol, coin_id, price_usd, price_idr, change_24h, last_updated, timestamp
  RSS : title, link, summary, published, source, timestamp
"""

import os
import platform
from pathlib import Path
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from pyspark.sql.functions import current_timestamp, lit

# ── Konfigurasi path ──────────────────────────────────────────────────
SCRIPT_DIR = Path(__file__).parent.resolve()

def to_uri(p):
    """Konversi path ke string yang bisa dibaca Spark lokal (forward slash)."""
    return str(p).replace("\\", "/")

HDFS_API_PATH = "hdfs://localhost:8020/data/crypto/api/"
HDFS_RSS_PATH = "hdfs://localhost:8020/data/crypto/rss/"

# Pada Windows dengan spasi di path (misal "TIARA F.A"), Hadoop tidak bisa
# baca path dengan %20. Gunakan C:/sparkdata/ sebagai workaround.
# Di Linux/Mac atau Windows tanpa spasi, pakai sample_data/ di dalam repo.
_WIN_SPACES = platform.system() == "Windows" and " " in str(SCRIPT_DIR)
_BASE       = Path("C:/sparkdata") if _WIN_SPACES else SCRIPT_DIR / "lakehouse_data"

LOCAL_API_DIR   = Path("C:/sparkdata/api")              if _WIN_SPACES else SCRIPT_DIR / "sample_data" / "api"
LOCAL_RSS_DIR   = Path("C:/sparkdata/rss")              if _WIN_SPACES else SCRIPT_DIR / "sample_data" / "rss"
BRONZE_API_PATH = _BASE / "bronze" / "crypto_api"
BRONZE_RSS_PATH = _BASE / "bronze" / "crypto_rss"

# ── Inisialisasi SparkSession + Delta Lake ────────────────────────────
# Catatan: TIDAK set spark.hadoop.fs.defaultFS agar path lokal tetap lokal
builder = (
    SparkSession.builder
    .appName("Bronze-CryptoWatch")
    .master("local[*]")
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.driver.memory", "2g")
    .config("spark.ui.enabled", "false")
    .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
)

spark = configure_spark_with_delta_pip(
    builder,
    extra_packages=["io.delta:delta-spark_2.12:3.3.2"]
).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("=" * 65)
print("  BRONZE LAYER - CryptoWatch Lakehouse")
print("=" * 65)


def ingest_to_bronze(hdfs_path, local_dir, bronze_path, source_name):
    """
    Baca JSON dari HDFS (atau fallback lokal), tambah metadata, simpan ke Delta.
    Return jumlah records yang berhasil diingest.
    """
    print(f"\n  Ingest '{source_name}' data...")

    df = None
    using_fallback = False

    # Coba HDFS dulu
    try:
        df = spark.read.option("multiLine", True).json(hdfs_path)
        count = df.count()  # trigger action untuk cek koneksi
        print(f"  Sumber  : HDFS — {hdfs_path}")
        print(f"  Records : {count}")
    except Exception as e:
        print(f"  HDFS tidak aktif ({type(e).__name__})")
        df = None

    # Fallback ke lokal jika HDFS gagal
    if df is None:
        if local_dir.exists():
            local_uri = to_uri(local_dir)
            print(f"  Fallback : lokal — {local_uri}")
            df = spark.read.option("multiLine", True).json(local_uri)
            using_fallback = True
        else:
            print(f"  GAGAL: folder fallback tidak ditemukan: {local_dir}")
            return 0

    total = df.count()
    print(f"  Total records  : {total}")
    print(f"  Mode           : {'LOKAL (fallback)' if using_fallback else 'HDFS'}")

    if total == 0:
        print(f"  Tidak ada data, lewati.")
        return 0

    df.printSchema()

    # Tambah metadata wajib
    bronze_df = (
        df
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source", lit(source_name))
    )

    # Simpan ke Delta Lake (pakai file:// URI)
    bronze_uri = to_uri(bronze_path)
    bronze_df.write.format("delta").mode("append").save(bronze_uri)
    print(f"  Tersimpan ke Delta: {bronze_uri}")
    return total


# ── Ingest API (harga koin real-time) ────────────────────────────────
total_api = ingest_to_bronze(HDFS_API_PATH, LOCAL_API_DIR, BRONZE_API_PATH, "api")

# ── Ingest RSS (berita kripto) ────────────────────────────────────────
total_rss = ingest_to_bronze(HDFS_RSS_PATH, LOCAL_RSS_DIR, BRONZE_RSS_PATH, "rss")

# ── Verifikasi hasil ──────────────────────────────────────────────────
print("\n" + "=" * 65)
print("  RINGKASAN BRONZE LAYER")
print("=" * 65)

if total_api > 0:
    print(f"\n  Sample Bronze API (5 baris):")
    spark.read.format("delta").load(to_uri(BRONZE_API_PATH)).show(5, truncate=60)

if total_rss > 0:
    print(f"\n  Sample Bronze RSS (5 baris):")
    spark.read.format("delta").load(to_uri(BRONZE_RSS_PATH)).show(5, truncate=60)

print(f"\n  API records diingest : {total_api}")
print(f"  RSS records diingest : {total_rss}")
print(f"  Format               : Delta Lake (ACID, versioned)")
print(f"  Metadata ditambahkan : _ingested_at, _source")

print("\n" + "=" * 65)
print("  Bronze Layer selesai! Lanjutkan ke 02_silver.py")
print("=" * 65)

spark.stop()
