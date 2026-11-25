import json
import random
import time
from datetime import datetime, timezone
from kafka import KafkaProducer

TOPIC = "station-telemetry"
STATIONS = ["S101", "S102", "S103"]

producer = KafkaProducer(
    bootstrap_servers="kafka:29092",
    key_serializer=lambda k: k.encode("utf-8"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

print(f"Streaming EV telemetry to topic '{TOPIC}'... Press Ctrl+C to stop.")
try:
    while True:
        station = random.choice(STATIONS)
        # Occasionally spike temperature > 70C to trigger Spark overheating alerts
        temp = round(random.uniform(71.0, 84.0), 1) if random.random() < 0.2 else round(random.uniform(35.0, 62.0), 1)
        event = {
            "station_id": station,
            "voltage": round(random.uniform(220.0, 245.0), 1),
            "current_amp": round(random.uniform(15.0, 32.0), 1),
            "temp_c": temp,
            "kwh": round(random.uniform(0.5, 3.5), 2),
            "event_time": datetime.now(timezone.utc).isoformat(),
        }
        # Using station_id as key guarantees ordering per station inside its partition
        meta = producer.send(TOPIC, key=station, value=event).get(timeout=10)
        print(f"[SENT] partition={meta.partition} offset={meta.offset} key={station} temp={temp}C")
        time.sleep(1)
except KeyboardInterrupt:
    print("\nStopped producer.")
finally:
    producer.close()
