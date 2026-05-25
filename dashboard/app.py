import json
import os
from datetime import datetime
from flask import Flask, render_template, jsonify

app = Flask(__name__)

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")

def load_json(filename):
    filepath = os.path.join(DATA_DIR, filename)
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return None


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/data")
def api_data():
    # gold_results.json dihasilkan oleh lakehouse/04_export_gold.py
    # kalau belum di-generate, fallback ke spark_results.json lama
    gold_results  = load_json("gold_results.json")
    spark_results = load_json("spark_results.json")
    live_api      = load_json("live_api.json")
    live_rss      = load_json("live_rss.json")

    return jsonify({
        "spark_results": spark_results,
        "gold_results":  gold_results,   # data dari Gold Delta Lake
        "live_api":      live_api,
        "live_rss":      live_rss,
        "updated_at":    datetime.now().isoformat(),
    })


@app.route("/api/gold")
def api_gold():
    """Data Gold Delta Lake langsung — 4 tabel hasil lakehouse pipeline."""
    gold_results = load_json("gold_results.json")
    if gold_results is None:
        return jsonify({
            "error": "Gold data belum tersedia. Jalankan lakehouse/04_export_gold.py terlebih dahulu."
        }), 404
    return jsonify(gold_results)


if __name__ == "__main__":
    os.makedirs(DATA_DIR, exist_ok=True)
    print("=" * 60)
    print("  CryptoWatch Dashboard — Kelompok 7")
    print("  http://localhost:5000")
    print("  API Gold Delta: http://localhost:5000/api/gold")
    print("=" * 60)
    app.run(host="0.0.0.0", port=5000, debug=False)
