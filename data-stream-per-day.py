import requests
from requests.auth import HTTPBasicAuth
from datetime import datetime, timezone
import re
import time
import getpass
from concurrent.futures import ThreadPoolExecutor, as_completed

# ================= CONFIG =================
MAX_WORKERS = 5
MAX_RETRIES = 2
RETRY_SLEEP = 1  # seconds

# ================= UTIL =================
def parse_size(size_str):
    if not size_str:
        return 0
    size_str = size_str.lower().strip()
    if size_str == "0b":
        return 0

    m = re.match(r"([\d\.]+)([a-z]+)", size_str)
    if not m:
        return 0

    value, unit = m.groups()
    value = float(value)

    units = {
        "b": 1,
        "kb": 1024,
        "mb": 1024 ** 2,
        "gb": 1024 ** 3,
        "tb": 1024 ** 4
    }
    return value * units.get(unit, 0)


def safe_parse_ts(ts):
    try:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        if dt.year < 1970 or dt.year > 2100:
            return None
        return dt
    except Exception:
        return None


def classify_status(age_days, last_seen_days):
    if last_seen_days > 3:
        return "STAGNANT"
    if age_days < 2:
        return "NEW/SHORT"
    return "ACTIVE"


def format_size_gb(value_gb):
    if value_gb >= 1:
        return f"{value_gb:.2f} GB"
    return f"{value_gb * 1024:.2f} MB"


def format_ingest(value_gb_per_day):
    if value_gb_per_day >= 1:
        return f"{value_gb_per_day:.2f} GB"
    return f"{value_gb_per_day * 1024:.2f} MB"


# ================= ELASTIC =================
def get_data_streams(es_url, auth):
    r = requests.get(f"{es_url}/_data_stream", auth=auth, timeout=30)
    r.raise_for_status()
    return [ds["name"] for ds in r.json()["data_streams"]]


def process_data_stream(es_url, auth, ds):
    session = requests.Session()
    session.auth = auth

    for attempt in range(1, MAX_RETRIES + 2):
        try:
            # ---------- AGE ----------
            query = {
                "size": 0,
                "aggs": {
                    "oldest": {"min": {"field": "@timestamp"}},
                    "newest": {"max": {"field": "@timestamp"}}
                }
            }

            r = session.get(f"{es_url}/{ds}/_search", json=query, timeout=30)
            if r.status_code != 200:
                return None

            aggs = r.json().get("aggregations", {})
            oldest = safe_parse_ts(aggs.get("oldest", {}).get("value_as_string", ""))
            newest = safe_parse_ts(aggs.get("newest", {}).get("value_as_string", ""))

            if not oldest or not newest:
                return None

            now = datetime.now(timezone.utc)
            age_days = (newest - oldest).total_seconds() / 86400
            last_seen_days = (now - newest).total_seconds() / 86400

            if age_days <= 0:
                return None

            status = classify_status(age_days, last_seen_days)

            # ---------- BACKING INDICES ----------
            r = session.get(f"{es_url}/_data_stream/{ds}", timeout=30)
            r.raise_for_status()
            indices = [i["index_name"] for i in r.json()["data_streams"][0]["indices"]]

            total_bytes = 0

            for idx in indices:
                r = session.get(
                    f"{es_url}/_cat/indices/{idx}",
                    params={"format": "json", "h": "dataset.size"},
                    timeout=30
                )
                if r.status_code == 200 and r.json():
                    total_bytes += parse_size(r.json()[0].get("dataset.size", "0b"))

                restored = f"partial-restored-{idx}"
                if session.head(f"{es_url}/{restored}", timeout=10).status_code == 200:
                    r = session.get(
                        f"{es_url}/_cat/indices/{restored}",
                        params={"format": "json", "h": "dataset.size"},
                        timeout=30
                    )
                    if r.status_code == 200 and r.json():
                        total_bytes += parse_size(r.json()[0].get("dataset.size", "0b"))

            if total_bytes == 0:
                return None

            size_gb = total_bytes / (1024 ** 3)
            ingest_rate = size_gb / age_days

            return {
                "ds": ds,
                "status": status,
                "size_gb": size_gb,
                "age_days": age_days,
                "last_seen_days": last_seen_days,
                "ingest_rate": ingest_rate
            }

        except Exception:
            if attempt <= MAX_RETRIES:
                time.sleep(RETRY_SLEEP * attempt)
                continue
            return None

    return None


# ================= MAIN =================
def main():
    es_url = input("Enter Elasticsearch URL: ").strip()
    username = input("Username: ").strip()
    password = getpass.getpass("Password: ")

    auth = HTTPBasicAuth(username, password)

    data_streams = get_data_streams(es_url, auth)
    total_ds = len(data_streams)

    print(f"\n{total_ds} data stream")
    print(f"Parallel workers: {MAX_WORKERS}\n")

    results = []
    completed = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(process_data_stream, es_url, auth, ds): ds
            for ds in data_streams
        }

        for future in as_completed(futures):
            completed += 1
            ds = futures[future]
            try:
                res = future.result()
                if res:
                    results.append(res)
                    print(f"[{completed}/{total_ds}] {ds} OK")
                else:
                    print(f"[{completed}/{total_ds}] {ds} SKIPPED")
            except Exception as e:
                print(f"[{completed}/{total_ds}] {ds} ERROR ({e})")

    # ---------- SORT ----------
    results.sort(key=lambda x: x["ingest_rate"], reverse=True)

    # ---------- EXCLUDE STAGNANT ----------
    results = [r for r in results if r["status"] != "STAGNANT"]

    # ---------- TABLE ----------
    COL_DS = 64
    COL_STATUS = 12
    COL_SIZE = 14
    COL_RET = 12
    COL_LAST = 12
    COL_INGEST = 16

    header = (
        f"{'Data Stream Name':<{COL_DS}}"
        f"{'Status':<{COL_STATUS}}"
        f"{'Stream Size':>{COL_SIZE}}"
        f"{'Retention':>{COL_RET}}"
        f"{'Last Data':>{COL_LAST}}"
        f"{'Ingest/Day':>{COL_INGEST}}"
    )

    line = "-" * (COL_DS + COL_STATUS + COL_SIZE + COL_RET + COL_LAST + COL_INGEST)

    print("\n=== DATA STREAM ACTIVITY ANALYSIS (NON-STAGNANT ONLY) ===")
    print(header)
    print(line)

    total_size = 0.0
    active_ingest = 0.0

    for r in results:
        total_size += r["size_gb"]
        if r["status"] == "ACTIVE":
            active_ingest += r["ingest_rate"]

        last_data = "Now" if r["last_seen_days"] < 1 else f"{int(r['last_seen_days'])}d ago"

        print(
            f"{r['ds']:<{COL_DS}}"
            f"{r['status']:<{COL_STATUS}}"
            f"{format_size_gb(r['size_gb']):>{COL_SIZE}}"
            f"{r['age_days']:>{COL_RET}.1f} d"
            f"{last_data:>{COL_LAST}}"
            f"{format_ingest(r['ingest_rate']):>{COL_INGEST}}"
        )

    print(line)
    print(f"TOTAL STORE SIZE (NON-STAGNANT STREAMS)        : {total_size:.2f} GB")
    print(f"ESTIMATED ACTIVE DATA INGESTION PER DAY        : {active_ingest:.2f} GB")
    print(f"TOTAL DATA STREAMS ANALYZED                    : {len(results)}")


if __name__ == "__main__":
    main()

