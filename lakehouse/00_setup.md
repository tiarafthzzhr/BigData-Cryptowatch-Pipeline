# Setup Guide — CryptoWatch Data Lakehouse

Panduan menjalankan Bronze → Silver → Gold pipeline di lokal.

---

## Prasyarat

| Komponen | Versi | Keterangan |
|----------|-------|-----------|
| Python | 3.10+ | |
| Java (JDK) | 17 | Wajib untuk PySpark |
| winutils *(Windows saja)* | Hadoop 3.3.x | Agar Spark bisa akses filesystem lokal |

---

## 1. Install Java 17

**Windows (PowerShell):**
```powershell
winget install Microsoft.OpenJDK.17
```
Setelah install, set environment variable:
```powershell
$env:JAVA_HOME = "C:\Program Files\Microsoft\jdk-17.0.19.10-hotspot"
```

**Linux/Mac:**
```bash
sudo apt install openjdk-17-jdk   # Ubuntu/Debian
brew install openjdk@17           # macOS
export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java))))
```

---

## 2. Setup winutils (Windows saja)

Spark di Windows membutuhkan `winutils.exe` dan `hadoop.dll` untuk mengakses filesystem lokal.

```powershell
# Buat folder
New-Item -ItemType Directory -Force "C:\winutils\bin"

# Download winutils + hadoop.dll (Hadoop 3.3.6)
$base = "https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.6/bin"
curl.exe -L "$base/winutils.exe" -o "C:\winutils\bin\winutils.exe"
curl.exe -L "$base/hadoop.dll"   -o "C:\winutils\bin\hadoop.dll"

# Set environment variable
$env:HADOOP_HOME = "C:\winutils"
$env:PATH = "C:\winutils\bin;" + $env:PATH
```

---

## 3. Buat Virtual Environment & Install Dependensi

```powershell
# Masuk ke folder lakehouse
cd lakehouse

# Buat venv khusus (jangan pakai venv utama proyek)
python -m venv lakehouse_venv

# Aktifkan & install
.\lakehouse_venv\Scripts\pip install "pyspark==3.5.3" "delta-spark==3.3.2"
```

**Linux/Mac:**
```bash
python3 -m venv lakehouse_venv
lakehouse_venv/bin/pip install "pyspark==3.5.3" "delta-spark==3.3.2"
```

---

## 4. Khusus Windows: Username dengan Spasi

Jika username Windows kamu mengandung spasi (misal `TIARA F.A`), Hadoop tidak dapat membaca path dengan spasi. Script otomatis mendeteksi kondisi ini dan menggunakan `C:\sparkdata\` sebagai direktori kerja.

Salin sample data ke `C:\sparkdata\`:
```powershell
New-Item -ItemType Directory -Force "C:\sparkdata\api"
New-Item -ItemType Directory -Force "C:\sparkdata\rss"
Copy-Item "lakehouse\sample_data\api\*" "C:\sparkdata\api\"
Copy-Item "lakehouse\sample_data\rss\*" "C:\sparkdata\rss\"
```

> Jika username **tidak** mengandung spasi, langkah ini tidak diperlukan — script akan otomatis baca dari `sample_data/`.

---

## 5. Jalankan Pipeline

Set environment variables terlebih dahulu (Windows):
```powershell
$env:JAVA_HOME        = "C:\Program Files\Microsoft\jdk-17.0.19.10-hotspot"
$env:HADOOP_HOME      = "C:\winutils"
$env:PATH             = "C:\winutils\bin;" + $env:PATH
$env:PYTHONUTF8       = "1"
$env:PYTHONIOENCODING = "utf-8"
```

Jalankan secara berurutan:
```powershell
# 1. Bronze — ingest JSON ke Delta Lake
.\lakehouse_venv\Scripts\python.exe 01_bronze.py

# 2. Silver — cleaning + Time Travel demo
.\lakehouse_venv\Scripts\python.exe 02_silver.py

# 3. Gold — agregasi + enhanced analysis
.\lakehouse_venv\Scripts\python.exe 03_gold.py
```

**Linux/Mac:**
```bash
lakehouse_venv/bin/python 01_bronze.py
lakehouse_venv/bin/python 02_silver.py
lakehouse_venv/bin/python 03_gold.py
```

---

## 6. Verifikasi Output

Setelah ketiga script selesai, Delta tables tersimpan di:

| Layer | Path (Windows+spasi) | Path (lainnya) |
|-------|---------------------|----------------|
| Bronze API | `C:\sparkdata\bronze\crypto_api` | `lakehouse_data/bronze/crypto_api` |
| Bronze RSS | `C:\sparkdata\bronze\crypto_rss` | `lakehouse_data/bronze/crypto_rss` |
| Silver API | `C:\sparkdata\silver\crypto_api` | `lakehouse_data/silver/crypto_api` |
| Silver RSS | `C:\sparkdata\silver\crypto_rss` | `lakehouse_data/silver/crypto_rss` |
| Gold Stats | `C:\sparkdata\gold\crypto_stats` | `lakehouse_data/gold/crypto_stats` |
| Gold Volatility | `C:\sparkdata\gold\crypto_hourly_volatility` | `lakehouse_data/gold/crypto_hourly_volatility` |
| Gold Spike Alerts | `C:\sparkdata\gold\crypto_spike_alerts` | `lakehouse_data/gold/crypto_spike_alerts` |
| Gold News Join | `C:\sparkdata\gold\crypto_news_price_join` | `lakehouse_data/gold/crypto_news_price_join` |

---

## Troubleshooting

| Error | Penyebab | Solusi |
|-------|---------|--------|
| `JAVA_HOME not set` | Java belum diinstall/env belum di-set | Set `$env:JAVA_HOME` (lihat langkah 1) |
| `UnsatisfiedLinkError: NativeIO$Windows` | `hadoop.dll` tidak ditemukan | Pastikan `C:\winutils\bin` ada di PATH |
| `PATH_NOT_FOUND` dengan `%20` | Spasi di path, winutils belum di PATH | Ikuti langkah 2 + 4 |
| `NullPointerException` di heartbeat | Versi PySpark/Delta tidak kompatibel | Gunakan PySpark 3.5.3 + delta-spark 3.3.2 |
| `Connection refused: localhost:8020` | HDFS tidak aktif | Normal — script otomatis fallback ke lokal |
