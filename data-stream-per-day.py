import requests
import json
import getpass
from requests.auth import HTTPBasicAuth
from datetime import datetime, timezone

def format_bytes(size):
    """Convert bytes to human-readable format (KB, MB, GB, TB)."""
    if size is None: return "0 B"
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size < 1024.0:
            return f"{size:.2f} {unit}"
        size /= 1024.0
    return f"{size:.2f} PB"

def get_time_range(url, auth, stream_name):
    """Fetch min and max timestamps using aggregation for high accuracy."""
    endpoint = f"{url}/{stream_name}/_search"
    query = {
        "size": 0,
        "aggs": {
            "oldest": {"min": {"field": "@timestamp"}},
            "newest": {"max": {"field": "@timestamp"}}
        }
    }
    try:
        res = requests.post(endpoint, auth=auth, json=query, verify=False, timeout=30)
        res.raise_for_status()
        aggs = res.json().get('aggregations', {})
        min_val = aggs.get('oldest', {}).get('value')
        max_val = aggs.get('newest', {}).get('value')
        if min_val and max_val:
            return min_val, max_val
    except:
        return None, None
    return None, None

def run_analyzer():
    print("\n--- Elasticsearch Ingestion, Size & Activity Analyzer ---")
    url = input("Enter Elasticsearch URL (e.g., https://localhost:9200): ").strip().rstrip('/')
    user = input("Enter Username: ").strip()
    password = getpass.getpass("Enter Password: ")
    auth = HTTPBasicAuth(user, password)
    
    # Get current time in epoch milliseconds
    now_ms = datetime.now(timezone.utc).timestamp() * 1000

    try:
        # Fetch basic data stream statistics
        stats_res = requests.get(f"{url}/_data_stream/_stats", auth=auth, verify=False)
        stats_res.raise_for_status()
        streams = stats_res.json().get('data_streams', [])

        processed_data = []
        print(f"\nAnalyzing {len(streams)} Data Streams...\n")

        for s in streams:
            name = s['data_stream']
            size_bytes = s['store_size_bytes']
            min_ts, max_ts = get_time_range(url, auth, name)
            
            status = "ACTIVE"
            days = 1.0
            last_ingest_days = 0
            
            if min_ts and max_ts:
                # 1. Calculate Data Range (Retention)
                delta_ms = max_ts - min_ts
                days = max(delta_ms / (1000 * 60 * 60 * 24), 0.1)
                
                # 2. Calculate Last Ingestion Time (Activity)
                last_ingest_ms = now_ms - max_ts
                last_ingest_days = last_ingest_ms / (1000 * 60 * 60 * 24)
                
                # Status Determination Logic
                if last_ingest_days > 3:
                    status = "STAGNANT"
                elif days < 5:
                    status = "NEW/SHORT"
            else:
                status = "EMPTY/UNKNOWN"

            # Ingest/Day is only calculated if data is active (not stagnant)
            rate_per_day = size_bytes / days if status != "STAGNANT" else 0
            
            processed_data.append({
                'name': name,
                'size': size_bytes,
                'retention': days,
                'last_seen': last_ingest_days,
                'rate': rate_per_day,
                'status': status
            })
            print(f"✓ {name[:40]:<40} | {status:<10} | {days:>6.1f} d range")

        # Sorting: Active status with highest rate first, STAGNANT last
        processed_data.sort(key=lambda x: (x['status'] == 'STAGNANT', -x['rate']))

        # Table Header
        print(f"\n{'Data Stream Name':<45} | {'Status':<10} | {'Stream Size':<12} | {'Retention':<10} | {'Last Data':<10} | {'Ingest/Day'}")
        print("-" * 125)

        for p in processed_data[:40]: # Display Top 40
            last_seen_str = f"{p['last_seen']:.1f}d ago" if p['last_seen'] > 0.1 else "Now"
            rate_str = format_bytes(p['rate']) if p['status'] != "STAGNANT" else "0 (Inactive)"
            
            print(f"{p['name'][:45]:<45} | {p['status']:<10} | {format_bytes(p['size']):<12} | {p['retention']:>6.1f} d  | {last_seen_str:<10} | {rate_str}")

        # Summary Logic
        total_size_bytes = sum(d['size'] for d in processed_data)
        total_active_rate = sum(d['rate'] for d in processed_data if d['status'] != "STAGNANT")
        
        print("-" * 125)
        print(f"{'TOTAL STORE SIZE (ALL STREAMS)':<45} : {format_bytes(total_size_bytes)}")
        print(f"{'ESTIMATED ACTIVE DATA GROWTH PER DAY':<45} : {format_bytes(total_active_rate)}")
        print(f"{'TOTAL DATA STREAMS ANALYZED':<45} : {len(processed_data)}")
        print("-" * 125)

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    
    run_analyzer()