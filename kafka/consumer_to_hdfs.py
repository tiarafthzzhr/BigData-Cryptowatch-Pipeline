"""
consumer_to_hdfs.py — Kafka Consumer → HDFS + Local JSON
# [Zahra Hafidzah]: Consumer yang membaca dari Kafka dan menyimpan ke HDFS

Membaca dari topic 'crypto-api' dan 'crypto-rss'.
Buffer events selama 2 menit, lalu flush ke HDFS sebagai JSON file.
Juga menyimpan salinan lokal untuk dashboard.
"""

import json
import os
import time
import subprocess
import threading
from datetime import datetime
from kafka import KafkaConsumer
from collections import defaultdict

# Konfigurasi
TOPICS = ["crypto-api", "crypto-rss"]
FLUSH_INTERVAL = 120  # 2 menit
DASHBOARD_DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "dashboard", "data")
HDFS_BASE = "/data/crypto"

# Pastikan folder dashboard/data ada
os.makedirs(DASHBOARD_DATA_DIR, exist_ok=True)

# Buffer untuk menyimpan events sebelum flush
api_buffer = []
rss_buffer = []
buffer_lock = threading.Lock()

def save_to_hdfs(local_path, hdfs_path):
    """Upload file lokal ke HDFS via docker exec + hdfs dfs -put (Bonus +2 Poin)."""
    try:
        # Copy file ke container hadoop-namenode
        subprocess.run(["docker", "cp", local_path, "hadoop-namenode:/tmp/upload.json"], timeout=15, check=True)
        # Pastikan folder HDFS ada
        hdfs_dir = os.path.dirname(hdfs_path)
        subprocess.run(["docker", "exec", "hadoop-namenode", "hdfs", "dfs", "-mkdir", "-p", hdfs_dir], timeout=15, stderr=subprocess.DEVNULL)
        # Upload ke HDFS (overwrite)
        subprocess.run(["docker", "exec", "hadoop-namenode", "hdfs", "dfs", "-put", "-f", "/tmp/upload.json", hdfs_path], timeout=30, check=True)
        print(f"  ✅ HDFS: {hdfs_path}")
    except Exception as e:
        print(f"  ❌ HDFS Error: {e}")

def flush_buffers():
    """Flush buffer ke file JSON lokal dan HDFS."""
    global api_buffer, rss_buffer
    
    with buffer_lock:
        api_data = api_buffer.copy()
        rss_data = rss_buffer.copy()
        api_buffer.clear()
        rss_buffer.clear()
    
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
    
    # Flush API data
    if api_data:
        # Simpan ke lokal untuk dashboard
        live_api_path = os.path.join(DASHBOARD_DATA_DIR, "live_api.json")
        with open(live_api_path, 'w') as f:
            json.dump(api_data, f, indent=2)
        print(f"  💾 Local: live_api.json ({len(api_data)} events)")
        
        # Simpan ke file timestamp untuk HDFS
        local_file = os.path.join(DASHBOARD_DATA_DIR, f"api_{timestamp}.json")
        with open(local_file, 'w') as f:
            json.dump(api_data, f)
        
        hdfs_path = f"{HDFS_BASE}/api/{timestamp}.json"
        save_to_hdfs(local_file, hdfs_path)
        
        # Hapus file timestamp lokal (sudah di HDFS)
        try:
            os.remove(local_file)
        except:
            pass
    
    # Flush RSS data
    if rss_data:
        live_rss_path = os.path.join(DASHBOARD_DATA_DIR, "live_rss.json")
        
        # --- Modifikasi Sliding Window (Simpan 20 artikel terbaru) ---
        existing_rss = []
        if os.path.exists(live_rss_path):
            try:
                with open(live_rss_path, 'r') as f:
                    existing_rss = json.load(f)
            except:
                pass
                
        all_rss = rss_data + existing_rss
        seen_links = set()
        unique_rss = []
        for item in all_rss:
            link = item.get("link", "")
            if link not in seen_links:
                seen_links.add(link)
                unique_rss.append(item)
                
        # Urutkan berdasarkan timestamp terbaru dan ambil maksimal 20
        unique_rss.sort(key=lambda x: x.get("timestamp", ""), reverse=True)
        final_rss = unique_rss[:20]
        
        with open(live_rss_path, 'w') as f:
            json.dump(final_rss, f, indent=2)
        print(f"  💾 Local: live_rss.json ({len(final_rss)} articles in view)")
        # -------------------------------------------------------------
        
        local_file = os.path.join(DASHBOARD_DATA_DIR, f"rss_{timestamp}.json")
        with open(local_file, 'w') as f:
            json.dump(rss_data, f)
        
        hdfs_path = f"{HDFS_BASE}/rss/{timestamp}.json"
        save_to_hdfs(local_file, hdfs_path)
        
        try:
            os.remove(local_file)
        except:
            pass
    
    if not api_data and not rss_data:
        print(f"  ℹ️  Buffer kosong, tidak ada yang di-flush")

def periodic_flush():
    """Flush buffer secara periodik."""
    while True:
        time.sleep(FLUSH_INTERVAL)
        ts = datetime.now().strftime("%H:%M:%S")
        print(f"\n[{ts}] ⏰ Flush buffer ke HDFS...")
        flush_buffers()

# Setup Kafka Consumer
consumer = KafkaConsumer(
    *TOPICS,
    bootstrap_servers=["localhost:9092"],
    group_id="hdfs-writer",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    key_deserializer=lambda k: k.decode("utf-8") if k else None,
    max_poll_records=100
)

print("=" * 60)
print("  📥 CryptoWatch — Consumer to HDFS")
print(f"  Topics: {TOPICS}")
print(f"  Flush interval: {FLUSH_INTERVAL}s")
print(f"  HDFS path: {HDFS_BASE}/")
print(f"  Dashboard dir: {DASHBOARD_DATA_DIR}")
print("=" * 60)

# Start periodic flush thread
flush_thread = threading.Thread(target=periodic_flush, daemon=True)
flush_thread.start()

api_count = 0
rss_count = 0
first_flush_done = False

try:
    for msg in consumer:
        with buffer_lock:
            if msg.topic == "crypto-api":
                api_buffer.append(msg.value)
                api_count += 1
            elif msg.topic == "crypto-rss":
                rss_buffer.append(msg.value)
                rss_count += 1
        
        total = api_count + rss_count
        if total % 10 == 0:
            ts = datetime.now().strftime("%H:%M:%S")
            print(f"[{ts}] Buffered: API={api_count} RSS={rss_count} (buf: API={len(api_buffer)} RSS={len(rss_buffer)})")
        
        # Flush segera setelah menerima batch pertama (minimal 5 events)
        if not first_flush_done and total >= 5:
            ts = datetime.now().strftime("%H:%M:%S")
            print(f"\n[{ts}] 🚀 Initial flush (first batch)...")
            flush_buffers()
            first_flush_done = True

except KeyboardInterrupt:
    pass

print(f"\n✋ Consumer selesai. Final flush...")
flush_buffers()
print(f"Total: API={api_count}, RSS={rss_count}")
consumer.close()

