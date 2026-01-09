# **Elasticsearch Ingest & Size Analyzer**

A Python-based monitoring tool designed to provide deep insights into Elasticsearch Data Streams. This script helps administrators identify storage-heavy indices, calculate real-world ingestion rates, and distinguish between active and stagnant data.

## **Features**

* **Accurate Retention Calculation**: Uses Elasticsearch aggregations (min and max on @timestamp) to determine the actual time range of data stored.  
* **Ingestion Rate Analytics**: Calculates the average data growth per day (Ingest/Day) for each data stream.  
* **Activity Status Classification**: Automatically categorizes streams as ACTIVE, STAGNANT, or NEW/SHORT based on ingestion patterns.  
* **Human-Readable Formatting**: Converts raw bytes into KB, MB, GB, or TB for easier analysis.  
* **Security**: Uses getpass for secure password entry and handles self-signed SSL certificates.

## **Logic & Methodology**

The analyzer calculates metrics based on the following logic:

### **1\. Data Retention (Range)**

The script determines the "age" of a data stream by finding the earliest and latest @timestamp across all backing indices.

* **Formula**: (Max Timestamp \- Min Timestamp) \= Retention Days  
* **Why?**: This shows how many days of history you are currently keeping in your cluster.

### **2\. Activity Status**

To help with capacity planning, the script assigns a status to each stream:

* **ACTIVE**: Data has been received within the last 3 days and the retention range is over 5 days.  
* **STAGNANT**: No new data has been ingested for more than 3 days. These are primary candidates for deletion or archiving to save disk space.  
* **NEW/SHORT**: The data stream is less than 5 days old. Ingestion rates for these might be skewed due to initial bulk loads.

### **3\. Ingestion Rate (Ingest/Day)**

This is the most critical metric for capacity planning.

* **Formula**: Total Store Size / Retention Days \= Ingest/Day  
* **Note**: The rate is set to 0 for **STAGNANT** streams to prevent "ghost" growth projections from inflating your daily estimates.

### **4\. Last Data (Recency)**

Calculates the delta between the current system time and the most recent document found. It tells you exactly how "fresh" the data in that stream is.

## **Prerequisites**

* Python 3.x  
* requests library

pip install requests

## **Usage**

1. Clone this repository.  
2. Run the script:  
   python analyzer.py

3. Provide your Elasticsearch URL (e.g., https://my-elasti-cluster:9200).  
4. Enter your credentials when prompted.

## **Example Output**

| Data Stream Name | Status | Stream Size | Retention | Last Data | Ingest/Day |
| :---- | :---- | :---- | :---- | :---- | :---- |
| logs-netflow.log-default | ACTIVE | 84.73 GB | 181.6 d | Now | 477.89 MB |
| traces-generic.otel-default | ACTIVE | 133.20 GB | 181.4 d | 0.2d ago | 752.17 MB |
| old-app-logs | STAGNANT | 50.00 GB | 90.0 d | 45.0d ago | 0 (Inactive) |

**Disclaimer**: This script disables SSL warnings by default to accommodate internal/self-signed certificates often found in local Elasticsearch deployments.
