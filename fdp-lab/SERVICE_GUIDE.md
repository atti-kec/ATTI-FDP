# FDP Big Data Lab - Service Architecture & Day-wise Execution Guide

This document lists all services, versions, startup commands, and day-wise container mappings tailored for the Faculty Development Program (FDP).

---

## Table 1: All Services, Versions & Startup Commands

| Service Name | Component / Role | Image & Version | Port Mappings | Dedicated Startup Command | Notes / Purpose |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **`lab`** | JupyterLab + PySpark + MLlib | `fdp-bigdata-lab:latest` (Spark 3.5.1, Python 3.8) | `8888:8888`<br>`4040:4040` | `docker compose up -d lab` | Interactive environment for all days. Spark local engine, connectors pre-baked. |
| **`namenode`** | HDFS Master (NameNode) | `apache/hadoop:3.3.6` | `9870:9870` | `docker compose --profile hadoop up -d namenode` | Distributed file system metadata & Web UI (`http://localhost:9870`). |
| **`datanode`** | HDFS Worker (DataNode) | `apache/hadoop:3.3.6` | None (internal HDFS ports) | `docker compose --profile hadoop up -d datanode` | Stores HDFS data blocks. |
| **`hive`** | Apache Hive (HiveServer2 / Beeline) | `apache/hive:4.0.0` | `10000:10000`<br>`10002:10002` | `docker compose --profile hadoop up -d hive` | HiveQL query engine & Web UI (`http://localhost:10002`). |
| **`mysql`** | Relational Database (RDBMS) | `mysql:8.0` | `3306:3306` | `docker compose --profile hadoop up -d mysql` | RDBMS source for Day 3 integration (using Spark JDBC as Sqoop replacement). Pre-seeded with `ev` database. |
| **`kafka`** | Real-time Stream Message Broker | `apache/kafka:3.8.0` | `9092:9092` | `docker compose --profile streaming up -d kafka` | Event streaming broker running in KRaft mode (no Zookeeper needed). |
| **`elasticsearch`** | Search & Analytics Indexing Engine | `docker.elastic.co/elasticsearch/elasticsearch:8.15.0` | `9200:9200` | `docker compose --profile elk up -d elasticsearch` | Stores & indexes streaming log records. Single-node mode with security disabled for workshop ease. |
| **`kibana`** | Visualization & Dashboard UI | `docker.elastic.co/kibana/kibana:8.15.0` | `5601:5601` | `docker compose --profile elk up -d kibana` | Real-time analytics dashboards (`http://localhost:5601`). |
| **`logstash`** | Log Processing Pipeline | `docker.elastic.co/logstash/logstash:8.15.0` | None (internal pipeline) | `docker compose --profile all up -d logstash` | Ingests from Kafka and indexes into Elasticsearch. |

---

## Table 2: Day-wise Service Requirements & Startup Profiles

To avoid high RAM usage and crashes on low-resource participant PCs (8 GB - 16 GB RAM), services are grouped into profiles so only the required containers are active each day:

| Day | Topic / Focus | Services Needed | Profile / Command to Run | Command to Stop Day's Services |
| :---: | :--- | :--- | :--- | :--- |
| **Day 1** | Big Data & Hadoop Ecosystem (HDFS) | `lab`, `namenode`, `datanode` | `docker compose --profile hadoop up -d lab namenode datanode` | `docker compose --profile hadoop stop` |
| **Day 2** | HDFS & Hive – Query Processing | `lab`, `namenode`, `datanode`, `hive` | `docker compose --profile hadoop up -d` | `docker compose --profile hadoop stop` |
| **Day 3** | RDBMS Integration & Spark (SQL/RDD) | `lab`, `namenode`, `datanode`, `hive`, `mysql` | `docker compose --profile hadoop up -d` | `docker compose --profile hadoop stop` |
| **Day 4** | Kafka – Real-Time Data Streaming | `lab`, `kafka` | `docker compose --profile streaming up -d` | `docker compose --profile streaming stop` |
| **Day 5** | Spark MLlib (Analytics & ML Models) | `lab`, `kafka` (if streaming ML) or `lab` only | `docker compose --profile streaming up -d` | `docker compose --profile streaming stop` |
| **Day 6** | ELK Stack & End-to-End Mini Project | `lab`, `kafka`, `elasticsearch`, `kibana`, `logstash` | `docker compose --profile all up -d` | `docker compose --profile all stop` |

---

## Memory & System Health Guidelines

- **Stopping vs. Deleting:**
  - `docker compose stop` keeps all downloaded images, containers, and data intact without freeing disk space or deleting tables.
  - Never use `docker system prune -a` or delete images during the workshop.
- **Resource Allocation in Docker Desktop:**
  - For Days 1–5: 4 GB RAM assigned to Docker Desktop is sufficient.
  - For Day 6 (ELK Stack): At least 6 GB (recommended 8 GB) RAM allocated to Docker Desktop.
- **Windows / WSL 2 Note for Elasticsearch (Day 6):**
  - Run once in PowerShell/WSL if Elasticsearch fails with virtual memory errors:
    ```bash
    wsl -d docker-desktop sysctl -w vm.max_map_count=262144
    ```
