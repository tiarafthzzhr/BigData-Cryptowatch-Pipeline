# 04_export_gold.py
# Baca 4 tabel Gold Delta → ekspor ke dashboard/data/gold_results.json
# Dijalankan setelah 03_gold.py, sebelum Flask dashboard

import json
import platform
from pathlib import Path
from datetime import datetime
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

SCRIPT_DIR = Path(__file__).parent.resolve()

def to_uri(p):
    return str(p).replace("\\", "/")

_WIN_SPACES = platform.system() == "Windows" and " " in str(SCRIPT_DIR)
_BASE = Path("C:/sparkdata") if _WIN_SPACES else SCRIPT_DIR / "lakehouse_data"

GOLD_STATS_PATH      = _BASE / "gold" / "crypto_stats"
GOLD_VOLATILITY_PATH = _BASE / "gold" / "crypto_hourly_volatility"
GOLD_SPIKE_PATH      = _BASE / "gold" / "crypto_spike_alerts"
GOLD_JOIN_PATH       = _BASE / "gold" / "crypto_news_price_join"

# output ke folder data dashboard Flask
DASHBOARD_DIR = SCRIPT_DIR.parent / "dashboard" / "data"
OUTPUT_FILE   = DASHBOARD_DIR / "gold_results.json"

builder = (
    SparkSession.builder
    .appName("Export-Gold-CryptoWatch")
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

print("=" * 65)
print("  EXPORT GOLD DELTA → JSON (untuk Flask Dashboard)")
print("=" * 65)


def df_to_records(df):
    return [row.asDict() for row in df.collect()]


print("\n  Membaca tabel Gold dari Delta Lake...")
gold_stats = spark.read.format("delta").load(to_uri(GOLD_STATS_PATH))
gold_vol   = spark.read.format("delta").load(to_uri(GOLD_VOLATILITY_PATH))
gold_spike = spark.read.format("delta").load(to_uri(GOLD_SPIKE_PATH))
gold_news  = spark.read.format("delta").load(to_uri(GOLD_JOIN_PATH))

print(f"  crypto_stats              : {gold_stats.count()} records")
print(f"  crypto_hourly_volatility  : {gold_vol.count()} records")
print(f"  crypto_spike_alerts       : {gold_spike.count()} records")
print(f"  crypto_news_price_join    : {gold_news.count()} records")

# konversi ke JSON — field analisis_1 dan analisis_2 sengaja disamakan
# dengan spark_results.json supaya frontend Flask tetap kompatibel
result = {
    "source": "Gold Delta Lake",
    "generated_at": datetime.now().isoformat(),

    # reproduksi ETS analisis 1 — struktur sama dengan spark_results.json
    "analisis_1_statistik_harga": [
        {
            "symbol":        r["symbol"],
            "avg_price":     float(r["avg_price_usd"]) if r["avg_price_usd"] else 0,
            "max_price":     float(r["max_price_usd"]) if r["max_price_usd"] else 0,
            "min_price":     float(r["min_price_usd"]) if r["min_price_usd"] else 0,
            "stddev_price":  float(r["stddev_price"])  if r["stddev_price"]  else 0,
            "total_records": int(r["total_records"]),
        }
        for r in df_to_records(gold_stats)
    ],

    # reproduksi ETS analisis 2 — sama dengan spark_results.json
    "analisis_2_volatilitas_per_jam": [
        {
            "jam":           int(r["jam"]),
            "avg_volatility": float(r["avg_volatility"]) if r["avg_volatility"] else 0,
            "max_volatility": float(r["max_volatility"]) if r["max_volatility"] else 0,
            "jumlah_data":   int(r["jumlah_data"]),
        }
        for r in df_to_records(gold_vol)
    ],

    # enhanced analisis 3 — BARU, tidak ada di ETS
    "analisis_3_spike_alerts": [
        {
            "symbol":     r["symbol"],
            "timestamp":  str(r["timestamp"]),
            "jam":        int(r["jam"]) if r["jam"] is not None else 0,
            "price_usd":  float(r["price_usd"])  if r["price_usd"]  else 0,
            "mean_price": float(r["mean_price"]) if r["mean_price"] else 0,
            "z_score":    float(r["z_score"])    if r["z_score"]    else 0,
        }
        for r in df_to_records(gold_spike)
    ],

    # enhanced analisis 4 — BARU, tidak ada di ETS
    "analisis_4_news_price_join": [
        {
            "symbol":               r["symbol"],
            "jam_spike":            int(r["jam_spike"]) if r["jam_spike"] is not None else 0,
            "jumlah_spike":         int(r["jumlah_spike"]),
            "avg_zscore":           float(r["avg_zscore"]) if r["avg_zscore"] else 0,
            "jumlah_berita_sejam":  int(r["jumlah_berita_sejam"]),
        }
        for r in df_to_records(gold_news)
    ],
}

DASHBOARD_DIR.mkdir(parents=True, exist_ok=True)
with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    json.dump(result, f, indent=2, default=str)

print(f"\n  Tersimpan: {OUTPUT_FILE}")
print(f"  analisis_1 (stats)     : {len(result['analisis_1_statistik_harga'])} simbol")
print(f"  analisis_2 (volatility): {len(result['analisis_2_volatilitas_per_jam'])} jam")
print(f"  analisis_3 (spikes)    : {len(result['analisis_3_spike_alerts'])} anomali")
print(f"  analisis_4 (news join) : {len(result['analisis_4_news_price_join'])} entri")

print("\n" + "=" * 65)
print("  Export selesai! Sekarang jalankan Flask: python dashboard/app.py")
print("  Cek endpoint: http://localhost:5000/api/gold")
print("=" * 65)

spark.stop()
