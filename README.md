# Elasticsearch Ingest Analyzer

A Python command-line tool to estimate **daily and monthly ingest rates** across all your Elasticsearch storage — both **data streams** and **regular (conventional) indices** — using primary shard statistics only.

---

## How It Works

The tool intelligently detects how your data is stored and applies the most accurate analysis method for each type:

| Storage Type | Detection Method | Analysis Method |
|---|---|---|
| **Data Stream** | `/_data_stream` API | Aggregate stats across all backing indices → filter by `@timestamp` |
| **Regular Index** | `/_cat/indices` API | Per-index stats → auto-detect timestamp field → filter by time range |

### Why two methods?

- **Data streams** store data across multiple backing indices. Calculating the average document size from the full aggregate gives a more representative result than per-backing-index calculation.
- **Regular indices** may or may not have a timestamp field, so the tool auto-detects it from the index mapping before querying.

### Excluded indices

The following index types are automatically excluded to prevent double-counting or irrelevant data:

- `partial-restored-*` — snapshot restore copies
- `restored-*` — manually restored indices
- `.` (dot-prefixed) — system indices (`.kibana`, `.security-*`, etc.)
- `ilm-history-*` — ILM history indices
- `shrink-*` — shrunk index copies
- Backing indices of data streams (`.ds-{name}-*`) — already counted via the data stream method

---

## Requirements

- Python **3.7+**
- [`requests`](https://pypi.org/project/requests/) library

Install the dependency:

```bash
pip install requests
```

---

## Usage

```bash
python ingest-analyzer.py
```

The tool will prompt you for the following inputs:

```
Elasticsearch URL (e.g. https://localhost:9200): https://your-cluster.elastic-cloud.com:443
Username: your_username
Password: ****************

--- Regular Index Filter (Optional) ---
Example: 'app-logs-*', 'metrics-*', or leave blank for all
Index pattern filter: 

--- Analysis Time Range ---
Start Date (YYYY-MM-DD): 2026-01-01
End Date (YYYY-MM-DD): 2026-04-20
```

### Input Reference

| Prompt | Description | Example |
|---|---|---|
| Elasticsearch URL | Full URL including port | `https://localhost:9200` |
| Username | Elasticsearch username | `elastic` |
| Password | Elasticsearch password (hidden input) | |
| Index pattern filter | Wildcard filter for regular indices only. Leave blank for all. | `app-logs-*` |
| Start Date | Beginning of analysis window | `2026-01-01` |
| End Date | End of analysis window (inclusive) | `2026-04-20` |

---

## Output

The tool runs in 4 steps and prints a structured report:

```
[1/4] Fetching data streams...        Found 173 data stream(s).
[2/4] Fetching regular indices...     Found 25 regular index/indices.
[3/4] Analyzing 173 data stream(s)...
  [DS]  logs-endpoint.events.process-default
  [DS]  traces-generic.otel-default
  ...
[4/4] Analyzing 25 regular index/indices...
  [IDX] bank_transactions (@timestamp)
  [IDX] customers (no-timestamp)
  ...
```

### Results Table — Data Streams

```
[ DATA STREAMS ] — 85 active
Name                                                    Range Docs   Est. Prim. Size        Ingest/Day
------------------------------------------------------------------------------------------------------
logs-endpoint.events.process-default                   156,812,284         169.45 GB           1.54 GB
traces-generic.otel-default                            255,869,894          49.51 GB         460.92 MB
...
```

### Results Table — Regular Indices

```
[ REGULAR INDICES ] — 12 active
Name                                                    Range Docs   Est. Prim. Size        Ingest/Day
------------------------------------------------------------------------------------------------------
bank_transactions_v2                                        28,320          15.02 MB           0.14 MB
...
```

### Grand Total

```
============================================================
  GRAND TOTAL (PRIMARY SHARDS ONLY)
============================================================
  Data Streams          : 3.24 GB/day
  Regular Indices       : 0.18 GB/day
  ──────────────────────────────────────────
  Total per Day         : 3.42 GB
  Total per Month (~30d): 102.60 GB

  Top 10 Highest Ingest Rate:
   1. [DS ] logs-endpoint.events.process-default           1.54 GB/day
   2. [DS ] traces-generic.otel-default                  460.92 MB/day
   ...
```

- `[DS]` — result from a data stream
- `[IDX]` — result from a regular index
- `Est. Prim. Size` — estimated size of documents within the date range (primary shards only, no replicas)
- `Ingest/Day` — average daily ingest rate over the selected period

---

## Configuration

You can adjust the following constant at the top of the script:

```python
MAX_WORKERS = 5  # Number of parallel threads for analysis
```

Increase this value for faster analysis on large clusters, or decrease it to reduce load on the Elasticsearch cluster.

---

## Notes

- **Self-signed certificates**: SSL verification is disabled (`verify=False`) to support self-signed or private CA certificates. A warning suppression is applied automatically.
- **No timestamp field**: Regular indices without a detectable timestamp field are still analyzed, but the document count reflects **all documents** rather than a time-filtered subset. These are marked as `no timestamp field, counted all docs` in the processing log.
- **Primary shards only**: All size estimates are based on primary shards only, excluding replicas. This gives a clean view of actual data volume without duplication from replication.
- **Closed indices**: Indices with `status: close` are automatically skipped since they cannot be queried.
- **Elasticsearch compatibility**: Tested against Elasticsearch 8.x. Compatible with Elastic Cloud and self-managed deployments.

---

## Example — Connecting to Elastic Cloud

```
Elasticsearch URL: https://my-deployment.es.us-east-1.aws.elastic-cloud.com:443
Username: elastic
Password: <your-cloud-password>
```

## Example — Connecting to Self-Managed (HTTP)

```
Elasticsearch URL: http://192.168.1.100:9200
Username: elastic
Password: <your-password>
```

---

## File Structure

```
.
├── unified-ingest-analyzer.py   # Main script
└── README.md                    # This file
```
