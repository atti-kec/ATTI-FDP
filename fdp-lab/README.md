# FDP Big Data Lab (Docker)

One folder, one command per day. No installing Hadoop, Hive, Spark or Kafka on teachers' PCs.

## One-time setup (every PC)
1. Install **Docker Desktop** (Windows: keep "Use WSL 2" ticked). In Settings > Resources give Docker **at least 6 GB RAM** (8 GB for Day 6).
2. Unzip this folder and open a terminal inside it.
3. Download images once on good internet (about 6-8 GB): `docker compose --profile all pull` then `docker compose build`.
   Slow college Wi-Fi? Do this at home or on one PC, then copy images to the others with `docker save` / `docker load`.

## Start per day
| Day | Command | What you get |
|---|---|---|
| 1-3 HDFS, Hive, MySQL, Spark | `docker compose --profile hadoop up -d` | HDFS, Hive, MySQL + Jupyter |
| 4-5 Kafka, Spark Streaming, MLlib | `docker compose --profile streaming up -d` | Kafka + Jupyter |
| 6 ELK + mini project | `docker compose --profile all up -d` | everything |

Stop (keeps data): `docker compose stop`. Remove everything: `docker compose down`.

## Links
| Tool | URL |
|---|---|
| Jupyter (notebooks, PySpark) | http://localhost:8888 |
| Spark UI (while a job runs) | http://localhost:4040 |
| HDFS NameNode | http://localhost:9870 |
| Hive web UI | http://localhost:10002 |
| Kibana | http://localhost:5601 |

## Handy commands
```bash
# HDFS (Day 1)
docker compose exec namenode hdfs dfs -mkdir -p /user/hive/warehouse /tmp
docker compose exec namenode hdfs dfs -ls /
# Hive (Day 2)
docker compose exec hive beeline -u jdbc:hive2://localhost:10000
# MySQL (Day 3)
docker compose exec mysql mysql -uroot -proot ev -e "select * from subscriptions;"
# Kafka (Day 4), run inside the kafka container
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --create --topic station-telemetry --partitions 3 --bootstrap-server localhost:9092
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --describe --topic station-telemetry --bootstrap-server localhost:9092
docker compose exec -it kafka /opt/kafka/bin/kafka-console-producer.sh --topic station-telemetry --bootstrap-server localhost:9092
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --topic station-telemetry --from-beginning --group alert-team --bootstrap-server localhost:9092
```
Inside Jupyter use these addresses: Kafka `kafka:29092`, HDFS `hdfs://namenode:8020`, MySQL `mysql:3306`, Elasticsearch `http://elasticsearch:9200`.
Run `notebooks/00_smoke_test.py` to check each service.

## Notes
- **Sqoop:** Apache retired Sqoop in 2021 and there is no official image. Day 3 import/export is done with **Spark JDBC** (`spark.read.jdbc`, example in the smoke test), the modern replacement. Explain Sqoop concepts on slides.
- **Elasticsearch will not start on Windows:** open a WSL terminal and run `sudo sysctl -w vm.max_map_count=262144`.
- **Hive or HDFS errors right after start:** wait 30-60 seconds and retry (NameNode needs time to leave safe mode).
- Hive uses an embedded Derby metastore: tables vanish if the `hive` container is removed. Fine for a workshop.
- Ports busy? Change the left number in `docker-compose.yml` (for example `"8889:8888"`).
