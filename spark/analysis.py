"""
analysis.py — Apache Spark Analysis untuk CryptoWatch
# [Tiara Fatimah A] Spark analysis - 3 analisis wajib dari HDFS

Membaca data dari HDFS, melakukan 3 analisis wajib:
1. Statistik harga per koin (avg, max, min, stddev)
2. Volatilitas per jam (avg |change_24h| per hour)
3. Volume berita per jam (count RSS per hour)

Hasil disimpan ke HDFS dan dashboard/data/spark_results.json
"""

import json
import os
import sys
import time
import subprocess
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, count, max as spark_max, min as spark_min,
    stddev, abs as spark_abs, hour, to_timestamp, round as spark_round,
    lit, when, cast
)
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans

# Konfigurasi
HDFS_API_PATH = "hdfs://localhost:8020/data/crypto/api/"
HDFS_RSS_PATH = "hdfs://localhost:8020/data/crypto/rss/"
HDFS_OUTPUT_PATH = "hdfs://localhost:8020/data/crypto/hasil/"
DASHBOARD_DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "dashboard", "data")
ANALYSIS_INTERVAL = 120  # 2 menit

os.makedirs(DASHBOARD_DATA_DIR, exist_ok=True)

# Inisialisasi SparkSession
spark = SparkSession.builder \
    .appName("CryptoWatch Analysis - Kelompok 7") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:8020") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Pastikan folder HDFS /data/crypto/hasil/ sudah ada
try:
    subprocess.run([
        "docker", "exec", "hadoop-namenode",
        "hdfs", "dfs", "-mkdir", "-p", "/data/crypto/hasil/"
    ], timeout=15, stderr=subprocess.DEVNULL)
    print("  ✅ HDFS folder /data/crypto/hasil/ siap.")
except Exception as e:
    print(f"  ⚠️ Gagal membuat folder HDFS: {e}")

print("=" * 70)
print("  ⚡ CryptoWatch — Spark Analysis (Kelompok 7)")
print("  🔄 Mode: Continuous (setiap 2 menit)")
print("=" * 70)

# ====================================================================
# LOOP UTAMA — Spark berjalan otomatis setiap 2 menit
# ====================================================================
cycle = 0
while True:
    cycle += 1
    results = {}
    print(f"\n{'=' * 70}")
    print(f"  ⏰ Siklus #{cycle} — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'=' * 70}")

    # ====================================================================
    # ANALISIS 1: Statistik Harga Per Koin
    # ====================================================================
    print("\n📊 ANALISIS 1: Statistik Harga Per Koin")
    print("-" * 50)

    try:
        df_api = spark.read.option("multiLine", True).json(HDFS_API_PATH)
        df_api = df_api.filter(col("symbol").isNotNull() & col("timestamp").isNotNull())
        print(f"Total records API: {df_api.count()}")

        # Register sebagai SQL view
        df_api.createOrReplaceTempView("crypto_prices")

        # DataFrame API
        stats = df_api.groupBy("symbol").agg(
            spark_round(avg("price_usd"), 2).alias("avg_price"),
            spark_round(spark_max("price_usd"), 2).alias("max_price"),
            spark_round(spark_min("price_usd"), 2).alias("min_price"),
            spark_round(stddev("price_usd"), 2).alias("stddev_price"),
            count("*").alias("total_records")
        ).orderBy(col("avg_price").desc())

        stats.show()

        # Narasi interpretasi
        print("📝 Interpretasi:")
        print("   Tabel di atas menunjukkan statistik harga ketiga koin kripto.")
        print("   Standar deviasi mengindikasikan seberapa berfluktuasi harga koin tersebut.")
        print("   Semakin tinggi stddev, semakin volatile koin tersebut.")

        results["analisis_1_statistik_harga"] = [row.asDict() for row in stats.collect()]

    except Exception as e:
        print(f"  ⚠️  Error Analisis 1: {e}")
        results["analisis_1_statistik_harga"] = []

    # ====================================================================
    # ANALISIS 2: Volatilitas Per Jam (Spark SQL)
    # ====================================================================
    print("\n📊 ANALISIS 2: Volatilitas Per Jam")
    print("-" * 50)

    try:
        # Spark SQL
        volatility = spark.sql("""
            SELECT 
                HOUR(TO_TIMESTAMP(timestamp)) as jam,
                ROUND(AVG(ABS(change_24h)), 4) as avg_volatility,
                COUNT(*) as jumlah_data,
                ROUND(MAX(ABS(change_24h)), 4) as max_volatility
            FROM crypto_prices
            GROUP BY HOUR(TO_TIMESTAMP(timestamp))
            ORDER BY jam
        """)

        volatility.show(24)

        print("📝 Interpretasi:")
        print("   Analisis ini menunjukkan rata-rata volatilitas harga kripto per jam.")
        print("   Jam dengan avg_volatility tinggi menunjukkan periode perdagangan paling aktif.")
        print("   Informasi ini berguna untuk menentukan waktu terbaik trading atau monitoring.")

        results["analisis_2_volatilitas_per_jam"] = [row.asDict() for row in volatility.collect()]

    except Exception as e:
        print(f"  ⚠️  Error Analisis 2: {e}")
        results["analisis_2_volatilitas_per_jam"] = []

    # ====================================================================
    # ANALISIS 3: Volume Berita Per Jam
    # ====================================================================
    print("\n📊 ANALISIS 3: Volume Berita Per Jam")
    print("-" * 50)

    try:
        df_rss = spark.read.option("multiLine", True).json(HDFS_RSS_PATH)
        df_rss = df_rss.filter(col("timestamp").isNotNull())
        print(f"Total records RSS: {df_rss.count()}")

        df_rss.createOrReplaceTempView("crypto_news")

        # Spark SQL
        news_volume = spark.sql("""
            SELECT 
                HOUR(TO_TIMESTAMP(timestamp)) as jam,
                COUNT(*) as jumlah_artikel,
                COUNT(DISTINCT source) as jumlah_sumber
            FROM crypto_news
            GROUP BY HOUR(TO_TIMESTAMP(timestamp))
            ORDER BY jumlah_artikel DESC
        """)

        news_volume.show(24)

        # Identifikasi jam paling aktif
        peak = spark.sql("""
            SELECT 
                HOUR(TO_TIMESTAMP(timestamp)) as jam,
                COUNT(*) as jumlah
            FROM crypto_news
            GROUP BY HOUR(TO_TIMESTAMP(timestamp))
            ORDER BY jumlah DESC
            LIMIT 1
        """)

        if peak.count() > 0:
            peak_row = peak.first()
            print(f"📝 Interpretasi:")
            print(f"   Jam paling aktif berita kripto: {peak_row['jam']}:00")
            print(f"   Dengan {peak_row['jumlah']} artikel.")
            print(f"   Korelasi volume berita dengan pergerakan harga dapat menjadi")
            print(f"   sinyal early warning untuk investor pemula.")

        results["analisis_3_volume_berita"] = [row.asDict() for row in news_volume.collect()]

    except Exception as e:
        print(f"  ⚠️  Error Analisis 3: {e}")
        results["analisis_3_volume_berita"] = []

    # ====================================================================
    # ANALISIS 4 (BONUS): Clustering K-Means dengan Spark MLlib
    # ====================================================================
    print("\n📊 ANALISIS 4 (BONUS): Clustering K-Means")
    print("-" * 50)

    try:
        # Siapkan data untuk MLlib
        # Hapus null values dan cast ke float
        ml_data = df_api.select(
            col("symbol"), 
            col("price_usd").cast("float").alias("price"), 
            col("change_24h").cast("float").alias("change")
        ).na.drop()

        if ml_data.count() > 3:
            # Assemble features
            assembler = VectorAssembler(inputCols=["price", "change"], outputCol="features")
            dataset = assembler.transform(ml_data)

            # Train KMeans model (K=3)
            kmeans = KMeans().setK(3).setSeed(1).setFeaturesCol("features")
            model = kmeans.fit(dataset)
            
            # Prediksi cluster
            predictions = model.transform(dataset)
            
            # Tampilkan hasil pengelompokan
            print("Pusat Cluster (Centroids):")
            centers = model.clusterCenters()
            for i, center in enumerate(centers):
                print(f"  Cluster {i}: Harga=${center[0]:.2f}, Perubahan={center[1]:.2f}%")
                
            print("\n📝 Interpretasi MLlib:")
            print("   Algoritma K-Means secara otomatis mengelompokkan data harga kripto")
            print("   menjadi 3 cluster berbeda berdasarkan rentang harga dan volatilitasnya.")
            print("   Ini berguna untuk mendeteksi anomali atau mengkategorikan aset.")
            
            # Simpan hasil agregasi tiap cluster
            cluster_stats = predictions.groupBy("prediction").agg(
                count("*").alias("jumlah_titik"),
                spark_round(avg("price"), 2).alias("avg_price")
            ).orderBy("prediction")
            
            results["analisis_4_kmeans_clusters"] = [row.asDict() for row in cluster_stats.collect()]
        else:
            print("  ⚠️ Data terlalu sedikit untuk K-Means clustering.")
            results["analisis_4_kmeans_clusters"] = []

    except Exception as e:
        print(f"  ⚠️  Error Analisis 4 (MLlib): {e}")
        results["analisis_4_kmeans_clusters"] = []

    # ====================================================================
    # Simpan Hasil
    # ====================================================================
    print("\n💾 Menyimpan hasil analisis...")

    # Simpan ke lokal untuk dashboard
    output_path = os.path.join(DASHBOARD_DATA_DIR, "spark_results.json")
    with open(output_path, 'w') as f:
        json.dump(results, f, indent=2, default=str)
    print(f"  ✅ Local: {output_path}")

    # Simpan ke HDFS
    try:
        local_tmp = "/tmp/spark_results.json"
        with open(local_tmp, 'w') as f:
            json.dump(results, f, indent=2, default=str)

        subprocess.run(["docker", "cp", local_tmp, "hadoop-namenode:/tmp/spark_results.json"], timeout=15)
        # Hapus file lama di HDFS beserta file _COPYING_ yang nyangkut (jika ada)
        subprocess.run([
            "docker", "exec", "hadoop-namenode",
            "hdfs", "dfs", "-rm", "-f", "/data/crypto/hasil/spark_results.json", "/data/crypto/hasil/spark_results.json._COPYING_"
        ], timeout=30, stderr=subprocess.DEVNULL)
        # Masukkan file baru
        subprocess.run([
            "docker", "exec", "hadoop-namenode",
            "hdfs", "dfs", "-put", "-f", "/tmp/spark_results.json", "/data/crypto/hasil/spark_results.json"
        ], timeout=30)
        print(f"  ✅ HDFS: /data/crypto/hasil/spark_results.json")
    except Exception as e:
        print(f"  ⚠️  HDFS save error: {e}")

    print(f"\n{'=' * 70}")
    print(f"  ✅ Siklus #{cycle} selesai!")
    print(f"  ⏳ Menunggu {ANALYSIS_INTERVAL} detik sebelum analisis berikutnya...")
    print(f"{'=' * 70}")
    time.sleep(ANALYSIS_INTERVAL)
