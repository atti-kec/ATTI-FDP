@echo off
echo =======================================================
echo  Exporting Day 6 Docker Images for Offline Lab (USB)
echo =======================================================

echo [1/3] Building local Jupyter + Spark lab image...
docker compose -f docker-compose.day6.yml build lab

echo [2/3] Pulling required Day 6 images...
docker pull apache/kafka:3.8.0
docker pull docker.elastic.co/elasticsearch/elasticsearch:8.15.0
docker pull docker.elastic.co/kibana/kibana:8.15.0
docker pull docker.elastic.co/logstash/logstash:8.15.0

echo [3/3] Saving all Day 6 images into day6_offline_images.tar...
docker save -o day6_offline_images.tar ^
  fdp-bigdata-day6-lab:latest ^
  apache/kafka:3.8.0 ^
  docker.elastic.co/elasticsearch/elasticsearch:8.15.0 ^
  docker.elastic.co/kibana/kibana:8.15.0 ^
  docker.elastic.co/logstash/logstash:8.15.0

echo =======================================================
echo Export complete!
echo Copy 'day6_offline_images.tar' and the 'fdp-lab' folder
echo to your USB drive for internet-free lab computers.
echo =======================================================
pause
