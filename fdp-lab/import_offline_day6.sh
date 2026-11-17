#!/usr/bin/env bash
set -e
echo "======================================================="
echo " Importing Day 6 Docker Images from Offline USB Archive"
echo "======================================================="

if [ ! -f day6_offline_images.tar ]; then
    echo "[ERROR] day6_offline_images.tar not found in current directory!"
    exit 1
fi

echo "Loading images into Docker..."
docker load -i day6_offline_images.tar

echo "Starting Day 6 Lab Environment..."
docker compose -f docker-compose.day6.yml up -d

echo "======================================================="
echo "Setup complete! Services active:"
echo " - JupyterLab:    http://localhost:8888"
echo " - Spark UI:      http://localhost:4040"
echo " - Elasticsearch: http://localhost:9200"
echo " - Kibana:        http://localhost:5601"
echo "======================================================="
