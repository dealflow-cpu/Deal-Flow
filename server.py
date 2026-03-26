"""
DealFlow Server
Serves the frontend app AND the scraped listings.json.
This is what runs on Render as a web service.
"""
from flask import Flask, send_from_directory, jsonify
import os
import json
import subprocess
import threading

app = Flask(__name__, static_folder="public")

DATA_DIR = os.path.dirname(os.path.abspath(__file__))
LISTINGS_FILE = os.path.join(DATA_DIR, "listings.json")

@app.route("/")
def index():
    return send_from_directory("public", "index.html")

@app.route("/listings.json")
def listings():
    if os.path.exists(LISTINGS_FILE):
        with open(LISTINGS_FILE) as f:
            data = json.load(f)
        return jsonify(data)
    return jsonify({"listings": [], "total_listings": 0, "error": "No data yet. Scraper hasn't run."})

@app.route("/status")
def status():
    exists = os.path.exists(LISTINGS_FILE)
    info = {"scraper_has_run": exists}
    if exists:
        with open(LISTINGS_FILE) as f:
            data = json.load(f)
        info["total_listings"] = data.get("total_listings", 0)
        info["scraped_at"] = data.get("scraped_at", "unknown")
        info["sources"] = data.get("sources", {})
    return jsonify(info)

@app.route("/scrape", methods=["POST"])
def trigger_scrape():
    """Manually trigger a scrape (optional)."""
    def run():
        subprocess.run(["python", "scraper.py"], cwd=DATA_DIR)
    t = threading.Thread(target=run)
    t.start()
    return jsonify({"status": "Scraper started in background"})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
