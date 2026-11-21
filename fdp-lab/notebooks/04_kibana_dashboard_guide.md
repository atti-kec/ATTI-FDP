# Day 6: Real-Time Kibana Dashboard Guide
## Visual IoT Operations Command Center for EV Charging Stations

Once your streaming pipeline (`01_ev_telemetry_generator.py` and `03_pyspark_streaming_anomaly_detection.ipynb`) is running, follow this step-by-step guide to visualize live anomalies in Kibana.

---

### Step 1: Access Kibana
1. Open your browser and navigate to: **`http://localhost:5601`**
2. Click **Explore on my own** if prompted with a welcome screen.

---

### Step 2: Create a Data View (Index Pattern)
Elasticsearch stores the alert events in the index named **`station-alerts`**. To view them in Kibana:
1. Click the **left navigation menu (☰)** > **Management** > **Stack Management**.
2. Under **Kibana**, click **Data Views** (or *Index Patterns* in older UI versions).
3. Click the blue **Create data view** button.
4. Fill in the fields:
   * **Name**: `EV Station Alerts`
   * **Index pattern**: `station-alerts*`
   * **Timestamp field**: Select `@timestamp` or `timestamp` from the dropdown.
5. Click **Save data view to Kibana**.

---

### Step 3: Live Search with the Inverted Index (Discover)
1. In the left navigation menu, click **Analytics** > **Discover**.
2. Select your newly created **EV Station Alerts** data view.
3. In the top-right corner, change the time picker to **Last 15 minutes** and set **Auto-refresh** to **Every 5 seconds**.
4. **Teaching Moment (Inverted Index in action):**
   * In the KQL search bar, type:
     ```kql
     status: "FAULT" and temperature_c > 85
     ```
   * *Explain to the audience:* Elasticsearch did not scan millions of rows. Lucene checked the Inverted Index's **Term Dictionary** for `status: FAULT` and performed a fast intersection on the bitsets of matching Document IDs!

---

### Step 4: Build the Real-Time Operations Dashboard (Doc Values)
1. In the left menu, click **Analytics** > **Dashboard** > **Create dashboard**.
2. Click **Create visualization** (Lens).

#### Visual 1: Critical Overheating Gauge / Metric
* **Type**: Metric
* **Primary Metric**: Count of records
* **Filter**: Add filter `temperature_c > 85`
* **Label**: *Active Overheat Incidents*
* Click **Save and return**.

#### Visual 2: Real-Time Temperature Trend (Line Chart)
* **Type**: Line chart
* **Horizontal Axis (X-axis)**: `timestamp` (Date histogram, per 10 seconds)
* **Vertical Axis (Y-axis)**: `Average of temperature_c`
* **Break down by**: `station_id` (Top 5 values)
* *Explain to the audience (Doc Values in action):* Aggregating the average temperature over time runs directly against Lucene's columnar **Doc Values**, reading contiguous float arrays in OS page cache without decompressing raw JSON strings.
* Click **Save and return**.

#### Visual 3: Faults by Location / Station (Bar Chart)
* **Type**: Bar chart
* **Horizontal Axis**: `location` (or `station_id`)
* **Vertical Axis**: Count of alerts
* Click **Save and return**.

---

### Step 5: Save & Present
1. Click the **Save** button in the top right.
2. Title it: **`EV Charging Station AI Fault Command Center`**.
3. Toggle the dashboard refresh interval to **Auto-refresh (5s)**.
4. Watch new alerts appear and graphs update in real-time as your Python generator and Spark streaming job detect anomalies!
