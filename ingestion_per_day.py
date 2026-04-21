import requests
from requests.auth import HTTPBasicAuth
from datetime import datetime, timezone, timedelta
import getpass
from concurrent.futures import ThreadPoolExecutor, as_completed

# ================= CONFIG =================
MAX_WORKERS = 5

# Prefixes to always exclude (restored copies, system indices)
EXCLUDE_PREFIXES = (
    ".",
    "partial-restored-",
    "restored-",
    "ilm-history",
    "shrink-",
)

# ================= UTIL =================
def format_size(value_gb):
    if value_gb >= 1:
        return f"{value_gb:.2f} GB"
    return f"{value_gb * 1024:.2f} MB"

def print_section(title):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")

# ================= ELASTIC: DATA STREAM =================
def get_data_streams(es_url, auth):
    """Retrieve all existing data streams."""
    r = requests.get(f"{es_url}/_data_stream", auth=auth, verify=False, timeout=30)
    r.raise_for_status()
    return {ds["name"] for ds in r.json()["data_streams"]}

def process_data_stream(es_url, auth, ds_name, start_dt, end_dt):
    """
    Analyze a data stream using the aggregate primary shards method.
    More accurate because avg_doc_size is calculated across all backing indices.
    """
    session = requests.Session()
    session.auth = auth
    session.verify = False

    try:
        # Stats aggregated across all backing indices
        r_stats = session.get(f"{es_url}/{ds_name}/_stats", timeout=30)
        if r_stats.status_code != 200:
            return None

        primaries = r_stats.json()["_all"]["primaries"]
        total_bytes_primary = primaries["docs"].get("total_size_in_bytes", 0)
        total_docs_primary = primaries["docs"].get("count", 0)

        if total_docs_primary == 0:
            return None

        avg_doc_size_bytes = total_bytes_primary / total_docs_primary

        # Count documents within the time range
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
        r_count = session.post(f"{es_url}/{ds_name}/_count", json=query, timeout=30)
        if r_count.status_code != 200:
            return None

        range_doc_count = r_count.json().get("count", 0)
        if range_doc_count == 0:
            return None

        est_range_size_gb = (range_doc_count * avg_doc_size_bytes) / (1024 ** 3)
        delta = end_dt - start_dt
        days_diff = max(delta.days + (delta.seconds / 86400), 1)
        ingest_per_day_gb = est_range_size_gb / days_diff

        return {
            "name": ds_name,
            "type": "data_stream",
            "range_docs": range_doc_count,
            "est_size_gb": est_range_size_gb,
            "ingest_rate_gb": ingest_per_day_gb,
            "ts_field": "@timestamp",
        }

    except Exception:
        return None

# ================= ELASTIC: REGULAR INDEX =================
def get_regular_indices(es_url, auth, data_stream_names, filter_pattern=None):
    """
    Retrieve all regular indices (not data streams, not restored copies).
    Backing indices from data streams (format .ds-*) are also excluded
    since they are already counted via the data stream method.
    """
    pattern = filter_pattern if filter_pattern else "*"
    url = f"{es_url}/_cat/indices/{pattern}?format=json&h=index,status"

    r = requests.get(url, auth=auth, verify=False, timeout=30)
    r.raise_for_status()

    # Build backing index prefixes from known data streams
    backing_index_prefixes = tuple(f".ds-{ds}-" for ds in data_stream_names)

    regular = []
    for item in r.json():
        name = item.get("index", "")
        status = item.get("status", "")

        # Skip closed indices (cannot be queried)
        if status == "close":
            continue

        # Skip system & restored prefixes
        if any(name.startswith(p) for p in EXCLUDE_PREFIXES):
            continue

        # Skip backing indices belonging to data streams
        if any(name.startswith(p) for p in backing_index_prefixes):
            continue

        # Skip if the name itself is a data stream (already handled)
        if name in data_stream_names:
            continue

        regular.append(name)

    return sorted(regular)


def detect_timestamp_field(session, es_url, index):
    """
    Detect the available timestamp field from the index mapping.
    Priority: @timestamp -> timestamp -> event.created -> time -> date
    Returns None if no timestamp field is found.
    """
    candidates = ["@timestamp", "timestamp", "event.created", "time", "date"]

    try:
        r = session.get(f"{es_url}/{index}/_mapping", timeout=15)
        if r.status_code != 200:
            return None

        all_props = {}
        for idx_data in r.json().values():
            props = idx_data.get("mappings", {}).get("properties", {})
            all_props.update(props)

        for candidate in candidates:
            parts = candidate.split(".")
            current = all_props
            found = True
            for part in parts:
                if part in current:
                    current = current[part].get("properties", current[part])
                else:
                    found = False
                    break
            if found:
                return candidate

    except Exception:
        pass

    return None


def process_regular_index(es_url, auth, index_name, start_dt, end_dt):
    """
    Analyze a regular index with automatic timestamp field detection.
    Falls back to counting all documents if no timestamp field is found.
    """
    session = requests.Session()
    session.auth = auth
    session.verify = False

    try:
        r_stats = session.get(f"{es_url}/{index_name}/_stats", timeout=30)
        if r_stats.status_code != 200:
            return None

        primaries = r_stats.json()["_all"]["primaries"]
        total_bytes_primary = primaries["docs"].get("total_size_in_bytes", 0)
        total_docs_primary = primaries["docs"].get("count", 0)

        if total_docs_primary == 0:
            return None

        avg_doc_size_bytes = total_bytes_primary / total_docs_primary

        ts_field = detect_timestamp_field(session, es_url, index_name)

        if ts_field:
            query = {
                "query": {
                    "range": {
                        ts_field: {
                            "gte": start_dt.isoformat(),
                            "lte": end_dt.isoformat()
                        }
                    }
                }
            }
            note = f"filtered by {ts_field}"
        else:
            query = {"query": {"match_all": {}}}
            note = "no timestamp field, counted all docs"

        r_count = session.post(f"{es_url}/{index_name}/_count", json=query, timeout=30)
        if r_count.status_code != 200:
            return None

        range_doc_count = r_count.json().get("count", 0)
        if range_doc_count == 0:
            return None

        est_range_size_gb = (range_doc_count * avg_doc_size_bytes) / (1024 ** 3)
        delta = end_dt - start_dt
        days_diff = max(delta.days + (delta.seconds / 86400), 1)
        ingest_per_day_gb = est_range_size_gb / days_diff

        return {
            "name": index_name,
            "type": "regular",
            "range_docs": range_doc_count,
            "est_size_gb": est_range_size_gb,
            "ingest_rate_gb": ingest_per_day_gb,
            "ts_field": ts_field,
            "note": note,
        }

    except Exception:
        return None

# ================= OUTPUT =================
def print_table(results, title, col_name_width=55):
    col_docs = 15
    col_size = 18
    col_rate = 18
    total_width = col_name_width + col_docs + col_size + col_rate

    header = (
        f"{'Name':<{col_name_width}}"
        f"{'Range Docs':>{col_docs}}"
        f"{'Est. Prim. Size':>{col_size}}"
        f"{'Ingest/Day':>{col_rate}}"
    )
    line = "-" * total_width

    print(f"\n{title}")
    print(header)
    print(line)

    total_daily = 0.0
    for r in results:
        total_daily += r["ingest_rate_gb"]
        print(
            f"{r['name']:<{col_name_width}}"
            f"{r['range_docs']:>{col_docs},}"
            f"{format_size(r['est_size_gb']):>{col_size}}"
            f"{format_size(r['ingest_rate_gb']):>{col_rate}}"
        )

    print(line)
    return total_daily

# ================= MAIN =================
def main():
    print("=== Elasticsearch Unified Ingest Analyzer ===")
    print("(Data Streams + Regular Indices | Primary Shards Only)\n")

    es_url = input("Elasticsearch URL (e.g. https://localhost:9200): ").strip().rstrip("/")
    username = input("Username: ").strip()
    password = getpass.getpass("Password: ")

    print("\n--- Regular Index Filter (Optional) ---")
    print("Example: 'app-logs-*', 'metrics-*', or leave blank for all")
    filter_pattern = input("Index pattern filter: ").strip() or None

    print("\n--- Analysis Time Range ---")
    start_str = input("Start Date (YYYY-MM-DD): ").strip()
    end_str = input("End Date (YYYY-MM-DD): ").strip()

    try:
        start_dt = datetime.strptime(start_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        end_dt = (
            datetime.strptime(end_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            + timedelta(days=1, seconds=-1)
        )
    except ValueError:
        print("Invalid date format! Please use YYYY-MM-DD.")
        return

    auth = HTTPBasicAuth(username, password)
    requests.packages.urllib3.disable_warnings()

    # ---- Step 1: Fetch Data Streams ----
    print("\n[1/4] Fetching data streams...")
    try:
        data_stream_names = get_data_streams(es_url, auth)
        print(f"      Found {len(data_stream_names)} data stream(s).")
    except Exception as e:
        print(f"      Failed to fetch data streams: {e}")
        data_stream_names = set()

    # ---- Step 2: Fetch Regular Indices ----
    print("[2/4] Fetching regular indices...")
    try:
        regular_indices = get_regular_indices(es_url, auth, data_stream_names, filter_pattern)
        print(f"      Found {len(regular_indices)} regular index/indices.")
    except Exception as e:
        print(f"      Failed to fetch regular indices: {e}")
        regular_indices = []

    # ---- Step 3: Analyze Data Streams ----
    ds_results = []
    if data_stream_names:
        print(f"\n[3/4] Analyzing {len(data_stream_names)} data stream(s)...")
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {
                executor.submit(process_data_stream, es_url, auth, ds, start_dt, end_dt): ds
                for ds in data_stream_names
            }
            for future in as_completed(futures):
                res = future.result()
                if res:
                    ds_results.append(res)
                    print(f"  [DS]  {res['name']}")
        ds_results.sort(key=lambda x: x["ingest_rate_gb"], reverse=True)
    else:
        print("\n[3/4] No data streams found, skipping.")

    # ---- Step 4: Analyze Regular Indices ----
    reg_results = []
    if regular_indices:
        print(f"\n[4/4] Analyzing {len(regular_indices)} regular index/indices...")
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {
                executor.submit(process_regular_index, es_url, auth, idx, start_dt, end_dt): idx
                for idx in regular_indices
            }
            for future in as_completed(futures):
                res = future.result()
                if res:
                    reg_results.append(res)
                    ts_info = res.get("ts_field") or "no-timestamp"
                    print(f"  [IDX] {res['name']} ({ts_info})")
        reg_results.sort(key=lambda x: x["ingest_rate_gb"], reverse=True)
    else:
        print("\n[4/4] No regular indices found, skipping.")

    # ---- Output ----
    print_section(f"ANALYSIS RESULTS: {start_str} to {end_str}")

    total_ds_daily = 0.0
    total_reg_daily = 0.0

    if ds_results:
        total_ds_daily = print_table(
            ds_results,
            f"[ DATA STREAMS ] — {len(ds_results)} active",
            col_name_width=55
        )
        print(f"  Subtotal Data Streams /day   : {format_size(total_ds_daily)}")

    if reg_results:
        total_reg_daily = print_table(
            reg_results,
            f"\n[ REGULAR INDICES ] — {len(reg_results)} active",
            col_name_width=55
        )
        print(f"  Subtotal Regular Indices /day: {format_size(total_reg_daily)}")

    # Grand Total
    grand_total = total_ds_daily + total_reg_daily
    print_section("GRAND TOTAL (PRIMARY SHARDS ONLY)")
    print(f"  Data Streams          : {format_size(total_ds_daily)}/day")
    print(f"  Regular Indices       : {format_size(total_reg_daily)}/day")
    print(f"  {'─'*42}")
    print(f"  Total per Day         : {format_size(grand_total)}")
    print(f"  Total per Month (~30d): {format_size(grand_total * 30)}")

    # Top 10 overall
    all_results = ds_results + reg_results
    all_results.sort(key=lambda x: x["ingest_rate_gb"], reverse=True)
    print(f"\n  Top 10 Highest Ingest Rate:")
    for i, r in enumerate(all_results[:10], 1):
        label = "DS " if r["type"] == "data_stream" else "IDX"
        print(f"  {i:>2}. [{label}] {r['name'][:58]:<58} {format_size(r['ingest_rate_gb'])}/day")

if __name__ == "__main__":
    main()
