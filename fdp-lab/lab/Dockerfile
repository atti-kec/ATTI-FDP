# Jupyter + Spark 3.5.1 (local mode) + MLlib + Kafka/MySQL connectors, all pre-installed
FROM apache/spark:3.5.1-python3
USER root
RUN apt-get update && apt-get install -y --no-install-recommends python3-pip \
    && rm -rf /var/lib/apt/lists/*
RUN pip3 install --no-cache-dir jupyterlab pandas numpy matplotlib scikit-learn \
    kafka-python "elasticsearch==8.15.0" pymysql

# Connector jars baked in so classrooms do not depend on live Maven downloads
COPY jars/*.jar /opt/spark/jars/
RUN chmod 644 /opt/spark/jars/*.jar

ENV PYSPARK_PYTHON=python3 \
    PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip
WORKDIR /workspace
EXPOSE 8888 4040
CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root", \
     "--ServerApp.token=", "--ServerApp.password=", "--notebook-dir=/workspace"]
