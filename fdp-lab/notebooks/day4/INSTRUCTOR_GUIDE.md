# Day 4 Instructor Guide & Side Notes (Full-Day Teaching Plan)

Use this guide as your **private speaker notes** while projecting `day4_class_guide.html` on the main screen.

---

## Full-Day Schedule & Pacing (~6 Hours)

| Time Block | Slide(s) | Activity | Instructor Talking Points & Actions |
| :--- | :--- | :--- | :--- |
| **09:30 – 10:15** | Slides 1–3 | **Lab PC Setup + Why Docker** | Have participants download WSL 3.0.5 + start `docker compose --profile streaming up -d`. While containers start, explain the "version mismatch" pain on Slide 3. |
| **10:15 – 11:00** | Slides 4–6 | **EV Story + Kafka Core** | Walk through point-to-point spaghetti vs. central log. Contrast MySQL (whiteboard) vs. Redis (post-it) vs. Kafka (bank passbook). Explain OS Page Cache & Binary TCP. |
| **11:00 – 11:15** | — | **Tea Break** | Ensure every PC shows `lab` and `kafka` in `docker ps`. |
| **11:15 – 12:00** | Slide 7 | **Hands-On #1 (Kafka CLI)** | Guide participants to create `station-telemetry` (`--partitions 3`) and open 2 terminals (`console-producer` and `console-consumer`). Have them type sample JSON. |
| **12:00 – 12:45** | Slides 8–9 | **Partitions + Python Producer** | Explain `hash(key) % 3` and Head/Tail. Walk through the `KafkaProducer` table, then have them open/run `01_producer_ev.py` (or type it in Jupyter). |
| **12:45 – 13:45** | — | **Lunch Break** | Leave `01_producer_ev.py` stopped or running as desired. |
| **13:45 – 14:45** | Slides 10–11 | **Consumer Groups Lab** | Demonstrate teamwork inside `alert-team` (run 2 terminals in same group and show each gets different partitions!) vs. `billing-team` (gets all). |
| **14:45 – 15:30** | Slide 12 | **Legacy vs. Modern Flow** | Cover ZooKeeper vs. KRaft, Medallion Architecture (Bronze/Silver/Gold), and alternatives (RabbitMQ, Flink, Redpanda). |
| **15:30 – 16:30** | Slides 13–14 | **Spark Streaming Labs + Challenge** | Emphasize **Spark IS the Consumer**. Run `03_spark_kafka_alerts.py` and `04_spark_window_agg.py`, inspect Spark UI (`localhost:4040`), and assign the Slide 14 filter challenge. |

---

## Step-by-Step Guide for Assisting Participants in Lab

### 1. Creating the Topic (Slide 7)
Ask everyone to run this in their `fdp-lab` terminal:
```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --create --if-not-exists --topic station-telemetry --partitions 3 --bootstrap-server localhost:9092
```
Verify it worked:
```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --describe --topic station-telemetry --bootstrap-server localhost:9092
```

### 2. Running Scripts in Terminal vs. JupyterLab
Participants have **two easy ways** to run the Day 4 Python files:
- **Option A (Terminal — Best for `01_producer_ev.py` & `02_consumer_group.py`):**
  ```bash
  docker compose exec lab python3 /workspace/day4/01_producer_ev.py
  ```
- **Option B (JupyterLab Browser — `http://localhost:8888`):**
  Navigate to `day4/` in the left file browser, open a Terminal inside JupyterLab (`File -> New -> Terminal`), and run:
  ```bash
  python3 /workspace/day4/03_spark_kafka_alerts.py
  ```
  *(Note: Running streaming scripts in a Jupyter Terminal or separate cell is cleaner than a normal notebook cell because streaming jobs run continuously until you press `Ctrl+C` or Stop).*

---

## Common Classroom Troubleshooting

1. **Error: `Connection refused` or `NoBrokersAvailable` inside Python/Spark:**
   - Inside Docker (`lab` container), always use `kafka:29092`.
   - From Windows host, use `localhost:9092`.
   - Wait 5–10 seconds after `up -d` for Kafka KRaft to finish booting.
2. **Spark Streaming prints empty batches or won't restart cleanly:**
   - Delete the old checkpoint folder inside `lab`:
     ```bash
     docker compose exec lab rm -rf /tmp/chk_ev_alerts /tmp/chk_ev_window_agg
     ```
3. **Consumer Group Lab (`02_consumer_group.py`) shows no messages:**
   - Make sure `01_producer_ev.py` is running in another terminal, OR check that `auto_offset_reset="earliest"` is set for a brand-new group name.
4. **Port `8888` or `9092` already in use:**
   - Edit `docker-compose.yml` left-side port (e.g., `"8889:8888"`) and run `docker compose --profile streaming up -d`.
