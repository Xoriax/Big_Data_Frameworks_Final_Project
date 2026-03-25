from flask import Flask, jsonify, request
from functools import wraps
import requests
import pyarrow.parquet as pq
import pandas as pd
import io
import os

app = Flask(__name__)

API_TOKEN = os.environ.get("API_TOKEN")

WEBHDFS = "http://namenode:9870/webhdfs/v1"

PATHS = {
    "humanitarian": "/data/gold/gold_output_humanitarian",
    "government":   "/data/gold/gold_output_government",
    "finance":      "/data/gold/gold_output_finance",
}

PAGE_SIZE = 5

def require_token(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        auth = request.headers.get("Authorization", "")
        if auth != "Bearer {}".format(API_TOKEN):
            return jsonify({"error": "Unauthorized"}), 401
        return f(*args, **kwargs)
    return decorated

def read_gold_table(hdfs_path):
    list_url = "{}{} ?op=LISTSTATUS".format(WEBHDFS, hdfs_path).replace(" ", "")
    r = requests.get(list_url, timeout=10)
    r.raise_for_status()
    statuses = r.json()["FileStatuses"]["FileStatus"]
    parquet_files = [s["pathSuffix"] for s in statuses if s["pathSuffix"].endswith(".parquet")]

    if not parquet_files:
        return []

    tables = []
    for fname in parquet_files:
        file_url = "{}{}/{}?op=OPEN".format(WEBHDFS, hdfs_path, fname)
        fr = requests.get(file_url, allow_redirects=True, timeout=30)
        fr.raise_for_status()
        tables.append(pq.read_table(io.BytesIO(fr.content)))

    import pyarrow as pa
    df = pa.concat_tables(tables).to_pandas()
    df = df.replace([float("inf"), float("-inf")], None)
    df = df.where(pd.notnull(df), None)
    return df.to_dict(orient="records")

def paginate(records):
    try:
        page = int(request.args.get("page", 1))
        if page < 1:
            page = 1
    except ValueError:
        page = 1

    total = len(records)
    total_pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)
    start = (page - 1) * PAGE_SIZE
    end = start + PAGE_SIZE

    return jsonify({
        "page": page,
        "page_size": PAGE_SIZE,
        "total": total,
        "total_pages": total_pages,
        "data": records[start:end]
    })

@app.route("/api/gold", methods=["GET"])
def index():
    return jsonify({
        "endpoints": [
            "/api/gold/humanitarian?page=1",
            "/api/gold/government?page=1",
            "/api/gold/finance?page=1"
        ]
    })

@app.route("/api/gold/humanitarian", methods=["GET"])
@require_token
def humanitarian():
    try:
        return paginate(read_gold_table(PATHS["humanitarian"]))
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/gold/government", methods=["GET"])
@require_token
def government():
    try:
        return paginate(read_gold_table(PATHS["government"]))
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/gold/finance", methods=["GET"])
@require_token
def finance():
    try:
        return paginate(read_gold_table(PATHS["finance"]))
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)