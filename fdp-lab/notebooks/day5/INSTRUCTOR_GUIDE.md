# Day 5 Instructor Guide & Mini-Project Coaching Plan

Use this as your **private speaker notes** alongside `day5_class_guide.html` (projected on screen).

---

## Full-Day Schedule (~6 Hours)

| Time Block | Slide(s) | Activity | Instructor Talking Points & Actions |
| :--- | :--- | :--- | :--- |
| **09:30 – 10:15** | Slides 1–3 | **Why Spark MLlib + Local Mode** | Recap Day 4, contrast Scikit-Learn (single PC OOM) vs Spark MLlib, explain `local[*]` & CPU cores/vCPUs cleanly. Verify `docker compose --profile streaming up -d`. |
| **10:15 – 11:00** | Slides 4–6 | **Preprocessing + Feature Pipelines + Lab #1** | Explain DataFrames, `Imputer`, `StringIndexer`, `VectorAssembler`, `StandardScaler`, and `Pipeline`. Run `01_preprocess_features.py` together. |
| **11:00 – 11:15** | — | **Tea Break** | — |
| **11:15 – 12:15** | Slides 7–9 | **Classification + Evaluation + Lab #2** | Cover Supervised vs Unsupervised, Logistic Regression vs Random Forest, Accuracy vs F1 vs AUC-ROC. Run `02_classification_fault.py` (this saves `/tmp/ev_fault_model`!). |
| **12:15 – 13:00** | Slides 10–12 | **Regression + Clustering + Labs #3 & #4** | Run `03_regression_energy.py` (RMSE, R²) and `04_clustering_stations.py` (K-Means + Silhouette). Walk through Slide 12 cheat sheet. |
| **13:00 – 14:00** | — | **Lunch Break** | — |
| **14:00 – 14:45** | Slides 13–14 | **Live Kafka + ML Integration (Lab #5)** | Show how `05_kafka_stream_ml.py` loads `/tmp/ev_fault_model` and scores live messages from `day4/01_producer_ev.py`. |
| **14:45 – 16:30** | Slides 15–16 | **Mini-Project Workshop (10 Scenarios)** | Divide faculty into pairs/groups, pick from the 10 scenarios on Slide 16, and build their 3-file pipeline (`producer.py`, `train_model.py`, `stream_predict.py`) ready for Day 6 ELK dashboards! |

---

## Quick Run Commands for You / Faculty

```bash
# Start containers (if not already running)
docker compose --profile streaming up -d

# Morning Hands-On (runs inside 'lab' container)
docker compose exec lab python3 /workspace/day5/01_preprocess_features.py
docker compose exec lab python3 /workspace/day5/02_classification_fault.py
docker compose exec lab python3 /workspace/day5/03_regression_energy.py
docker compose exec lab python3 /workspace/day5/04_clustering_stations.py

# Afternoon Live Kafka + ML Demo (2 terminals)
# Terminal 1:
docker compose exec lab python3 /workspace/day4/01_producer_ev.py
# Terminal 2:
docker compose exec lab python3 /workspace/day5/05_kafka_stream_ml.py
```

---

## Common Classroom Troubleshooting (Day 5)

1. **Error: `IllegalArgumentException: requirement failed: Column features must be of type VectorUDT`**
   - **Cause:** Participant forgot `VectorAssembler` or passed the raw column instead of `featuresCol="features"`.
   - **Fix:** Ensure `VectorAssembler(..., outputCol="features")` runs before the ML stage.
2. **Error: `SparkException: Unseen label: S104` during `StringIndexer` transform:**
   - **Cause:** A new category appeared in test/streaming data that wasn't seen during training.
   - **Fix:** Always include `handleInvalid="keep"` in `StringIndexer(...)`.
3. **Why save models to `/tmp/ev_fault_model` instead of `/workspace/`?**
   - Saving Hadoop/Spark Parquet models directly onto a Windows NTFS bind-mount (`/workspace`) can occasionally trigger Hadoop permission warnings on Windows Docker Desktop. Writing inside `/tmp/` in the Linux container is instant and error-free.
4. **Streaming ML script (`05_kafka_stream_ml.py`) stuck on old offsets:**
   - Clear checkpoint:
     ```bash
     docker compose exec lab rm -rf /tmp/chk_ev_ml_scoring
     ```
