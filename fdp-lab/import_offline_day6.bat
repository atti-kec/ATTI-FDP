@echo off
echo =======================================================
echo  Importing Day 6 Docker Images from Offline USB Archive
echo =======================================================

if not exist day6_offline_images.tar (
    echo [ERROR] day6_offline_images.tar not found in this directory!
    echo Please ensure the .tar file was copied from the USB stick.
    pause
    exit /b 1
)

echo Loading images into Docker Desktop... This may take 2-4 minutes.
docker load -i day6_offline_images.tar

echo Starting Day 6 Lab Environment...
docker compose -f docker-compose.day6.yml up -d

echo =======================================================
echo Setup complete! Lab services are up:
echo  - JupyterLab:    http://localhost:8888
echo  - Spark UI:      http://localhost:4040
echo  - Elasticsearch: http://localhost:9200
echo  - Kibana:        http://localhost:5601
echo =======================================================
pause
