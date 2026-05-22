# CryptoWatch — Data Lakehouse (Tugas Lanjutan ETS)

**Kelompok 7** | Big Data | 2025/2026  
Tema: **CryptoWatch** — Pipeline harga kripto real-time (BTC, ETH, BNB)

---

> **Mode Jalankan:**  
> Script secara otomatis mencoba HDFS terlebih dahulu.  
> Jika HDFS tidak aktif, script fallback ke `sample_data/` lokal (sudah disertakan).  
> Bronze layer maksimal 15 poin dalam mode lokal — tidak ada pengurangan poin lain.  
> Lihat [00_setup.md](00_setup.md) untuk detail lengkap.

---

## Daftar Isi
1. [Arsitektur: Sebelum vs Sesudah](#1-arsitektur-sebelum-vs-sesudah)
2. [Struktur Folder](#2-struktur-folder)
3. [Penjelasan Setiap Layer](#3-penjelasan-setiap-layer)
4. [Justifikasi Transformasi Silver](#4-justifikasi-transformasi-silver)
5. [Perbandingan Gold vs ETS Lama](#5-perbandingan-gold-vs-ets-lama)
6. [Demonstrasi Time Travel](#6-demonstrasi-time-travel)
7. [Refleksi: Delta Lake vs HDFS JSON](#7-refleksi-delta-lake-vs-hdfs-json)

---

## 1. Arsitektur: Sebelum vs Sesudah

### Sebelum (ETS)

```
[CoinGecko API]  ──►  Kafka (crypto-api)  ──►  Consumer  ──►  HDFS /data/crypto/api/*.json
[CoinDesk RSS]   ──►  Kafka (crypto-rss)  ──►  Consumer  ──►  HDFS /data/crypto/rss/*.json
                                                                          │
                                                                          ▼
                                                              spark/analysis.py
                                                           (baca JSON mentah langsung)
                                                                          │
                                                            ┌─────────────┴──────────────┐
                                                            │  Analisis 1: Statistik Harga│
                                                            │  Analisis 2: Volatilitas    │
                                                            │  Analisis 3: Volume Berita  │
                                                            │  Analisis 4: K-Means (bonus)│
                                                            └─────────────┬──────────────┘
                                                                          │
                                                                          ▼
                                                              dashboard/data/spark_results.json
                                                                  (Flask Dashboard)
```

**Masalah ETS:** Data JSON mentah — tidak ada schema enforcement, duplikat bisa masuk,
timestamp sebagai string, tidak ada versioning, tidak bisa query ulang data historis.

---

### Sesudah (Tugas Ini — Medallion Architecture)

```
[CoinGecko API]  ──►  Kafka (crypto-api)  ──►  Consumer  ──►  HDFS /data/crypto/api/*.json
[CoinDesk RSS]   ──►  Kafka (crypto-rss)  ──►  Consumer  ──►  HDFS /data/crypto/rss/*.json
                                                                          │
                                                                          ▼
                                                              lakehouse/01_bronze.py
                                                          + metadata: _ingested_at, _source
                                                                          │
                                                          ┌───────────────┴──────────────────┐
                                                          │       BRONZE LAYER (Delta)        │
                                                          │  lakehouse_data/bronze/crypto_api │
                                                          │  lakehouse_data/bronze/crypto_rss │
                                                          └───────────────┬──────────────────┘
                                                                          │
                                                                          ▼
                                                              lakehouse/02_silver.py
                                                           (5 transformasi cleaning)
                                                                          │
                                                          ┌───────────────┴──────────────────┐
                                                          │       SILVER LAYER (Delta)        │
                                                          │  lakehouse_data/silver/crypto_api │
                                                          │  lakehouse_data/silver/crypto_rss │
                                                          │  [ACID | Versioned | Time Travel] │
                                                          └───────────────┬──────────────────┘
                                                                          │
                                                                          ▼
                                                              lakehouse/03_gold.py
                                                           (4 tabel agregasi & enhanced)
                                                                          │
                                          ┌───────────────────────────────┴──────────────────────┐
                                          │                   GOLD LAYER (Delta)                  │
                                          │  crypto_stats          (Reproduksi ETS Analisis 1)    │
                                          │  crypto_hourly_volatility (Reproduksi ETS Analisis 2) │
                                          │  crypto_spike_alerts   (Enhanced: z-score > 2σ)       │
                                          │  crypto_news_price_join (Enhanced: cross-source join) │
                                          └───────────────────────────────┬──────────────────────┘
                                                                          │
                                                                          ▼
                                                              Flask Dashboard
                                                         (baca dari Gold, bukan JSON mentah)
```

---

## 2. Struktur Folder

```
lakehouse/
├── README_lakehouse.md        ← Dokumentasi ini
├── 00_setup.md                ← Cara install & menjalankan
├── 01_bronze.py               ← Ingest HDFS → Bronze Delta
├── 02_silver.py               ← Cleaning + Time Travel → Silver Delta
└── 03_gold.py                 ← Agregasi + Enhanced → Gold Delta

lakehouse_data/                ← Di-generate saat script berjalan
├── bronze/
│   ├── crypto_api/            ← Delta table (JSON mentah + metadata)
│   └── crypto_rss/
├── silver/
│   ├── crypto_api/            ← Delta table (bersih, versioned)
│   └── crypto_rss/
└── gold/
    ├── crypto_stats/
    ├── crypto_hourly_volatility/
    ├── crypto_spike_alerts/
    └── crypto_news_price_join/
```

---

## 3. Penjelasan Setiap Layer

### Bronze Layer — Raw + Metadata

Script: `01_bronze.py`

Bronze adalah salinan 1:1 dari data HDFS, **tidak ada transformasi pada nilai data**.
Yang ditambahkan hanya dua kolom metadata:

| Kolom | Tipe | Nilai |
|-------|------|-------|
| `_ingested_at` | TimestampType | Waktu script dijalankan |
| `_source` | StringType | `"api"` atau `"rss"` |

**Mengapa perlu Bronze?**  
Bronze menjadi *audit trail* — jika Silver atau Gold bermasalah, kita bisa
selalu trace kembali ke data asli tanpa perlu menarik ulang dari HDFS.

---

### Silver Layer — Cleaned + Versioned

Script: `02_silver.py`

Data dari Bronze dibersihkan dan siap dipakai untuk analisis.
Lihat [Justifikasi Transformasi Silver](#4-justifikasi-transformasi-silver).

---

### Gold Layer — Aggregated + Enriched

Script: `03_gold.py`

Tabel siap-pakai untuk dashboard dan laporan.

| Tabel | Deskripsi | Tipe |
|-------|-----------|------|
| `crypto_stats` | avg/min/max/stddev harga per simbol | Reproduksi ETS |
| `crypto_hourly_volatility` | rata-rata volatilitas (|change_24h|) per jam | Reproduksi ETS |
| `crypto_spike_alerts` | titik harga anomali (z-score > 2σ) | Enhanced |
| `crypto_news_price_join` | korelasi jumlah berita dengan jam spike | Enhanced |

---

## 4. Justifikasi Transformasi Silver

### API Data (Harga Koin)

#### Transformasi 1 — Cast timestamp → TimestampType
```python
api_clean = bronze_api.withColumn("timestamp", to_timestamp(col("timestamp")))
```
**Mengapa penting:**  
Di ETS, kolom `timestamp` berupa string ISO seperti `"2026-05-11T10:00:00"`.  
String tidak bisa langsung dipakai di Window Function (`lag`, `lead`, rolling avg)
maupun fungsi temporal (`hour()`, `date_trunc()`).  
Setelah di-cast ke `TimestampType`, semua operasi temporal menjadi akurat.

**Dampak jika tidak dilakukan:**  
- `hour(col("timestamp"))` menghasilkan `null` untuk string timestamp
- Window Function tidak bisa diurutkan secara kronologis
- Analisis Enhanced di Gold layer (z-score) tidak akurat

---

#### Transformasi 2 — Hapus Duplikat (symbol, timestamp)
```python
api_clean = api_clean.dropDuplicates(["symbol", "timestamp"])
```
**Mengapa penting:**  
Consumer melakukan flush buffer setiap 2 menit. Jika consumer direstart di tengah
siklus, data yang belum di-commit bisa masuk dua kali ke HDFS.
Kombinasi `(symbol, timestamp)` unik per pengukuran — tidak ada dua data harga
BTC yang bisa terjadi di detik yang persis sama.

**Dampak jika tidak dilakukan:**  
- `AVG(price_usd)` menjadi lebih tinggi/rendah dari kenyataan jika ada duplikat
- `COUNT(*)` inflated — total records tidak merepresentasikan jumlah pengukuran unik
- `STDDEV` terdistorsi karena nilai yang sama dihitung dua kali

**Berapa baris hilang?**  
Tergantung jumlah restart consumer. Dalam pengujian normal: 0–5% duplikat.

---

#### Transformasi 3 — Filter price_usd <= 0
```python
api_clean = api_clean.filter(col("price_usd") > 0)
```
**Mengapa penting:**  
CoinGecko API mengembalikan `0` atau value tidak valid saat:
- Rate limit tercapai (429 Too Many Requests)
- Jaringan timeout dan response tidak lengkap
- Koin sedang tidak aktif diperdagangkan

Harga kripto secara fundamental tidak mungkin nol atau negatif.
Data ini adalah artefak teknis, bukan data nyata.

**Dampak jika tidak dilakukan:**  
- `MIN(price_usd)` selalu 0 — tidak bermakna
- `AVG(price_usd)` tertarik ke bawah secara signifikan
- Deteksi anomali z-score menjadi salah karena mean terdistorsi

**Berapa baris hilang?**  
Biasanya < 1%. Hanya terjadi saat ada API error.

---

#### Transformasi 4 — Handle null change_24h → isi 0.0
```python
api_clean = api_clean.withColumn("change_24h",
    when(col("change_24h").isNull(), lit(0.0)).otherwise(col("change_24h")))
```
**Mengapa penting:**  
Spark: `AVG(null_col)` mengabaikan null dalam perhitungan, tapi `SUM` dan
beberapa fungsi Window mengembalikan `null` jika ada satu null di window.
Untuk analisis volatilitas per jam, satu `null` bisa membuat seluruh jam itu
mengembalikan `null` — baris itu hilang dari visualisasi dashboard.

Mengisi `0.0` berarti "perubahan tidak terdeteksi", yang secara bisnis lebih
aman daripada menghilangkan data titik harga tersebut.

---

#### Transformasi 5 — Ekstrak kolom 'jam'
```python
api_clean = api_clean.withColumn("jam", hour(col("timestamp")))
```
**Mengapa penting:**  
Precomputed column — daripada menghitung `hour(timestamp)` di setiap query
Gold, kita simpan nilainya sekali. Ini mempercepat query dan menghindari
repetisi kalkulasi. Berguna untuk semua analisis temporal (volatilitas per jam,
berita per jam, spike per jam).

---

### RSS Data (Berita Kripto)

#### Transformasi 1 — Cast timestamp → TimestampType
Sama seperti API — diperlukan untuk fungsi temporal.

#### Transformasi 2 — Hapus Duplikat (link)
```python
rss_clean = rss_clean.dropDuplicates(["link"])
```
**Mengapa penting:**  
RSS feed di-poll setiap 5 menit. Artikel yang diterbitkan pagi hari masih
muncul di feed siang hari. Producer memiliki dedup in-memory, tapi itu hilang
saat restart. `link` (URL) adalah primary key natural setiap artikel.

#### Transformasi 3 — Filter artikel tanpa judul
```python
rss_clean = rss_clean.filter(col("title").isNotNull() & (col("title") != ""))
```
**Mengapa penting:**  
Artikel tanpa judul adalah entry malformed dari RSS feed. Tidak bisa dianalisis
untuk topik atau sentimen. Jumlahnya biasanya < 1 dari total feed.

---

## 5. Perbandingan Gold vs ETS Lama

### Analisis 1: Statistik Harga Per Simbol

| Aspek | ETS (spark/analysis.py) | Gold (03_gold.py) |
|-------|------------------------|-------------------|
| Sumber data | JSON mentah HDFS | Silver Delta (bersih) |
| Duplikat | Bisa ada | Sudah dihapus |
| Nilai price=0 | Ikut dihitung | Sudah difilter |
| Akurasi AVG | Bisa terdistorsi | Akurat |
| Akurasi STDDEV | Bisa terdistorsi | Akurat |
| Reproducible | Tidak (overwrite hasil) | Ya (versioned) |

**Kesimpulan:** Gold menghasilkan statistik yang lebih akurat karena sumber datanya
sudah bersih dari duplikat dan nilai invalid.

---

### Analisis 2: Volatilitas Per Jam

| Aspek | ETS | Gold |
|-------|-----|------|
| Parsing timestamp | `HOUR(TO_TIMESTAMP(string))` — bisa gagal | `hour(TimestampType)` — akurat |
| Duplikat | Bisa double-count | Tidak ada |
| Kolom 'jam' | Dihitung ulang tiap query | Precomputed di Silver |
| Null change_24h | Bisa menghasilkan null row | Di-handle dengan 0.0 |

---

### Analisis Enhanced (BARU di Gold)

**Gold 3: Deteksi Spike Anomali (crypto_spike_alerts)**

Di ETS ini tidak bisa dilakukan karena:
1. Mean dan stddev tidak akurat (ada duplikat dan price=0)
2. Tidak ada baseline statistik yang tersimpan untuk dibandigkan
3. `price_usd` langsung dari JSON bisa `null` atau `0`

Di Gold, karena Silver sudah bersih, kita bisa hitung z-score yang valid:
```
z = (price_usd - mean_per_symbol) / stddev_per_symbol
```
Titik dengan `|z| > 2` adalah anomali statistik — harga yang "tidak biasa" 
secara historis untuk koin tersebut.

**Gold 4: Berita vs Spike (crypto_news_price_join)**

Di ETS tidak bisa dilakukan karena:
1. API dan RSS tersimpan di folder HDFS berbeda tanpa relasi
2. Timestamp API dan RSS masih string — tidak bisa di-join secara temporal
3. Tidak ada Silver layer bersih yang siap di-join

Di Gold, karena keduanya sudah di Silver dengan `TimestampType` dan kolom `jam`
yang sama, join bisa dilakukan berdasarkan jam yang sama:
```
Silver API (spike jam 10) JOIN Silver RSS (berita jam 10) → korelasi
```

---

## 6. Demonstrasi Time Travel

Demonstrasi ada di akhir `02_silver.py`.

### Alur Demonstrasi

```
1. Tulis Silver API (versi 0: data bersih)
         │
         ▼
2. Lihat history → version=0, operation=WRITE

3. UPDATE: change_24h = 0.0  →  -999.0
         │
         ▼
4. Lihat history → version=1, operation=UPDATE

5. Baca versi terbaru (v1):  change_24h = -999.0  ✓
   Baca versi 0 (time travel): change_24h = 0.0   ✓ (masih bisa diakses!)
         │
         ▼
6. Restore ke versi 0 (overwrite dengan data v0)
```

### Output yang Diharapkan

```
=== History Tabel Silver API ===
+-------+-------------------+---------+
|version|timestamp          |operation|
+-------+-------------------+---------+
|0      |2026-05-13 10:xx:xx|WRITE    |
+-------+-------------------+---------+

(setelah update)
+-------+-------------------+---------+
|version|timestamp          |operation|
+-------+-------------------+---------+
|1      |2026-05-13 10:xx:xx|UPDATE   |
|0      |2026-05-13 10:xx:xx|WRITE    |
+-------+-------------------+---------+

=== Data SEKARANG (v1 — setelah update) ===
+------+----------------+--------------+
|symbol|jumlah_sentinel |total_records |
+------+----------------+--------------+
|BTC   |3               |120           |
|ETH   |2               |120           |
|BNB   |1               |120           |
+------+----------------+--------------+

=== Data VERSI 0 (sebelum update — Time Travel) ===
+------+----------------+--------------+
|symbol|jumlah_change_0 |total_records |
+------+----------------+--------------+
|BTC   |3               |120           |
|ETH   |2               |120           |
|BNB   |1               |120           |
+------+----------------+--------------+
```

### Cara Akses Time Travel Secara Manual

```python
# Baca versi spesifik
df_v0 = spark.read.format("delta").option("versionAsOf", 0).load(SILVER_API_PATH)

# Baca berdasarkan timestamp
df_at_time = spark.read.format("delta") \
    .option("timestampAsOf", "2026-05-13 10:00:00") \
    .load(SILVER_API_PATH)
```

---

## 7. Refleksi: Keuntungan Delta Lake vs HDFS JSON

### Masalah HDFS JSON (ETS)

| Masalah | Dampak |
|---------|--------|
| Tidak ada schema enforcement | Kolom bisa hilang atau tipe berubah tanpa error |
| Tidak ada ACID | Jika Spark crash saat write, file korup |
| Tidak ada versioning | Update data = tidak bisa kembali ke versi sebelumnya |
| Tidak ada dedup bawaan | Harus manual setiap kali analisis |
| Append = append semua | Tidak bisa update satu baris saja |

### Keuntungan Delta Lake

| Fitur | Penjelasan | Contoh di Proyek Ini |
|-------|-----------|----------------------|
| **ACID Transactions** | Write atomik — tidak ada file korup | Bronze/Silver/Gold tidak bisa setengah-setengah |
| **Schema Enforcement** | Kolom baru yang tidak dikenal ditolak | Mencegah kolom `price_eth` masuk ke tabel `crypto_api` |
| **Time Travel** | Query data di versi mana pun | Demonstrasi di `02_silver.py` — restore ke v0 |
| **Versioning** | Setiap write/update/delete tercatat | `DeltaTable.history().show()` |
| **Merge/Upsert** | Update baris spesifik tanpa overwrite | Bisa update harga BTC tertentu tanpa sentuh ETH |
| **Partition Pruning** | Query hanya membaca partisi yang relevan | Filter per `jam` lebih cepat jika di-partition |
| **Audit Trail** | Rekam siapa & kapan mengubah data | Penting untuk compliance data keuangan |

### Kapan HDFS JSON Cukup?

- Prototyping cepat tanpa kebutuhan re-query
- Data yang tidak pernah di-update
- Tim kecil tanpa kebutuhan audit

### Kapan Delta Lake Wajib?

- Produksi dengan data keuangan (kripto, saham) — **wajib ACID**
- Pipeline yang berjalan 24/7 dan bisa restart kapan saja
- Kebutuhan debugging: "harga mana yang salah dan kapan masuknya?"
- Analisis temporal (Window Function) yang butuh timestamp akurat

---

*Kelompok 7 — Tiara Fatimah A., Zahra Hafidzah, Nafis Faqih Allmuzaky Maolidi, Mohamad Arkan Zahir A.*
