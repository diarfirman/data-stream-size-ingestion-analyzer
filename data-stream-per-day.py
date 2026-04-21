import requests
from requests.auth import HTTPBasicAuth
from datetime import datetime, timezone, timedelta
import time
import getpass
from concurrent.futures import ThreadPoolExecutor, as_completed

# ================= CONFIG =================
MAX_WORKERS = 5

# ================= UTIL =================
def format_size(value_gb):
    if value_gb >= 1:
        return f"{value_gb:.2f} GB"
    return f"{value_gb * 1024:.2f} MB"

# ================= ELASTIC =================
def get_data_streams(es_url, auth):
    # Menggunakan verify=False untuk menangani self-signed cert jika ada
    r = requests.get(f"{es_url}/_data_stream", auth=auth, verify=False, timeout=30)
    r.raise_for_status()
    return [ds["name"] for ds in r.json()["data_streams"]]

def process_ds_granular(es_url, auth, ds, start_dt, end_dt):
    session = requests.Session()
    session.auth = auth
    session.verify = False

    try:
        # 1. Ambil Stats khusus PRIMARIES (Langsung dari endpoint /_stats)
        r_stats = session.get(f"{es_url}/{ds}/_stats", timeout=30)
        if r_stats.status_code != 200:
            return None
        
        stats_json = r_stats.json()
        primaries = stats_json["_all"]["primaries"]
        
        # Menggunakan total_size_in_bytes dari dokumen primary
        # Ini mencakup data mentah + overhead indexing tanpa replika
        total_bytes_primary = primaries["docs"]["total_size_in_bytes"]
        total_docs_primary = primaries["docs"]["count"]

        if total_docs_primary == 0:
            return None

        # 2. Hitung Rata-rata Ukuran per Dokumen (Hanya Primary)
        avg_doc_size_bytes = total_bytes_primary / total_docs_primary

        # 3. Hitung Jumlah Dokumen dalam Rentang Waktu
        query = {
            "query": {
                "range": {
                    "@timestamp": {
                        "gte": start_dt.isoformat(),
                        "lte": end_dt.isoformat()
                    }
                }
            }
        }
        r_count = session.post(f"{es_url}/{ds}/_count", json=query, timeout=30)
        if r_count.status_code != 200:
            return None
        
        range_doc_count = r_count.json().get("count", 0)

        # 4. Kalkulasi Akhir
        est_range_size_gb = (range_doc_count * avg_doc_size_bytes) / (1024 ** 3)
        
        # Hitung selisih hari
        delta = end_dt - start_dt
        days_diff = delta.days + (delta.seconds / 86400)
        if days_diff <= 0: days_diff = 1
        
        ingest_per_day_gb = est_range_size_gb / days_diff

        return {
            "ds": ds,
            "range_docs": range_doc_count,
            "est_size_gb": est_range_size_gb,
            "ingest_rate_gb": ingest_per_day_gb
        }

    except Exception:
        return None

# ================= MAIN =================
def main():
    print("=== Elasticsearch Primary-Only Ingest Analyzer ===")
    es_url = input("Enter Elasticsearch URL (e.g https://localhost:9200): ").strip()
    username = input("Username: ").strip()
    password = getpass.getpass("Password: ")
    
    print("\n--- Rentang Waktu Analisis ---")
    start_str = input("Start Date (YYYY-MM-DD): ")
    end_str = input("End Date (YYYY-MM-DD): ")
    
    try:
        start_dt = datetime.strptime(start_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        # End date ditambahkan 1 hari agar mencakup akhir hari yang dipilih
        end_dt = datetime.strptime(end_str, "%Y-%m-%d").replace(tzinfo=timezone.utc) + timedelta(days=1, seconds=-1)
    except ValueError:
        print("Format tanggal salah! Gunakan YYYY-MM-DD.")
        return

    auth = HTTPBasicAuth(username, password)
    requests.packages.urllib3.disable_warnings() 

    try:
        data_streams = get_data_streams(es_url, auth)
    except Exception as e:
        print(f"Gagal mengambil data streams: {e}")
        return

    results = []
    print(f"\nMenganalisis {len(data_streams)} data streams (Primary Shards Only)...\n")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_ds_granular, es_url, auth, ds, start_dt, end_dt): ds for ds in data_streams}
        for future in as_completed(futures):
            res = future.result()
            if res:
                results.append(res)
                print(f"[OK] {res['ds']}")

    # Sort berdasarkan ingest harian tertinggi
    results.sort(key=lambda x: x["ingest_rate_gb"], reverse=True)

    # Table Output
    col_ds, col_docs, col_size, col_rate = 50, 15, 18, 18
    header = f"{'Data Stream Name':<{col_ds}}{'Range Docs':>{col_docs}}{'Est. Prim. Size':>{col_size}}{'Ingest/Day':>{col_rate}}"
    line = "-" * (col_ds + col_docs + col_size + col_rate)

    print(f"\nANALISIS PERIODE (PRIMARY ONLY): {start_str} s/d {end_str}")
    print(header)
    print(line)

    total_daily_primary_ingest = 0
    for r in results:
        total_daily_primary_ingest += r["ingest_rate_gb"]
        print(f"{r['ds']:<{col_ds}}{r['range_docs']:>{col_docs},}{format_size(r['est_size_gb']):>{col_size}}{format_size(r['ingest_rate_gb']):>{col_rate}}")

    print(line)
    print(f"ESTIMASI TOTAL INGESTION (PRIMARY) PER HARI: {total_daily_primary_ingest:.2f} GB")

if __name__ == "__main__":
    main()
