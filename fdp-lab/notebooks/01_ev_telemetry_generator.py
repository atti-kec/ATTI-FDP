#!/usr/bin/env python3
"""
01_ev_telemetry_generator.py
============================
Simulates real-time IoT sensor telemetry from a network of EV Smart Charging Stations.
Pushes events into Kafka topic 'station-telemetry'.

Can be run:
  1. From host machine: python 01_ev_telemetry_generator.py --bootstrap localhost:9092
  2. Inside lab container: python 01_ev_telemetry_generator.py --bootstrap kafka:29092
"""

import json
import time
import random
import argparse
from datetime import datetime, timezone
from kafka import KafkaProducer

STATIONS = [
    {"id": "S101", "location": "Perundurai-KEC"},
    {"id": "S102", "location": "Erode-BusStand"},
    {"id": "S103", "location": "Coimbatore-Gandhipuram"},
    {"id": "S104", "location": "Salem-NewBusStand"},
    {"id": "S105", "location": "Tiruppur-AvinashiRoad"},
    {"id": "S106", "location": "Bhavani-HighwayHub"},
    {"id": "S107", "location": "Namakkal-Collectorate"},
    {"id": "S108", "location": "Karur-Roundana"},
    {"id": "S109", "location": "Dharapuram-Central"},
    {"id": "S110", "location": "Anthiyur-Junction"},
]

def generate_telemetry(inject_anomaly=False):
    station = random.choice(STATIONS)
    now = datetime.now(timezone.utc).isoformat()

    if inject_anomaly:
        # Injected Anomaly: Thermal Runaway / Severe Voltage Drop Fault
        temp = round(random.uniform(85.0, 104.5), 2)
        voltage = round(random.uniform(280.0, 320.0), 2)
        current = round(random.uniform(140.0, 190.0), 2)
        power_kw = round((voltage * current) / 1000.0, 2)
        status = "FAULT"
        error_code = random.choice(["OVERHEAT_WARNING", "BATTERY_THERMAL_SURGE", "VOLTAGE_SAG_CRITICAL"])
    else:
        # Normal High-Power DC Fast Charging
        temp = round(random.uniform(32.0, 48.0), 2)
        voltage = round(random.uniform(380.0, 415.0), 2)
        current = round(random.uniform(30.0, 110.0), 2)
        power_kw = round((voltage * current) / 1000.0, 2)
        status = "CHARGING" if current > 5 else "IDLE"
        error_code = "NONE"

    return {
        "timestamp": now,
        "station_id": station["id"],
        "location": station["location"],
        "voltage_v": voltage,
        "current_a": current,
        "temperature_c": temp,
        "power_kw": power_kw,
        "status": status,
        "error_code": error_code
    }

def main():
    parser = argparse.ArgumentParser(description="EV Telemetry Stream Generator for Kafka")
    parser.add_argument("--bootstrap", default="kafka:29092", help="Kafka broker address (e.g. kafka:29092 or localhost:9092)")
    parser.add_argument("--topic", default="station-telemetry", help="Kafka topic name")
    parser.add_argument("--interval", type=float, default=0.5, help="Sleep interval in seconds between messages")
    parser.add_argument("--anomaly-rate", type=float, default=0.12, help="Probability (0.0 to 1.0) of injecting an anomaly")
    args = parser.parse_args()

    print(f"[+] Connecting to Kafka broker at: {args.bootstrap}")
    print(f"[+] Target topic: {args.topic}")
    print(f"[+] Emitting ~{int(1/args.interval)} msg/sec (Anomaly Probability: {args.anomaly_rate*100:.0f}%)")

    producer = None
    retries = 15
    while retries > 0:
        try:
            producer = KafkaProducer(
                bootstrap_servers=args.bootstrap,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks=1
            )
            print("[+] Successfully connected to Kafka!")
            break
        except Exception as e:
            print(f"[-] Kafka not ready yet ({e}). Retrying in 2 seconds...")
            time.sleep(2)
            retries -= 1

    if not producer:
        print("[!] Failed to connect to Kafka. Exiting.")
        return

    msg_count = 0
    anomaly_count = 0
    try:
        while True:
            is_anomaly = random.random() < args.anomaly_rate
            record = generate_telemetry(inject_anomaly=is_anomaly)
            
            # Key by station_id so same station always maps to same Kafka partition!
            producer.send(args.topic, key=record["station_id"], value=record)
            msg_count += 1
            if is_anomaly:
                anomaly_count += 1
                print(f" [!] ANOMALY [{record['station_id']} @ {record['location']}] Temp: {record['temperature_c']}C | Volt: {record['voltage_v']}V | Error: {record['error_code']}")
            else:
                if msg_count % 10 == 0:
                    print(f" [*] Streamed {msg_count} records ({anomaly_count} anomalies detected so far)...")
            
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print(f"\n[+] Stopped by user. Total sent: {msg_count} records ({anomaly_count} anomalies).")
    finally:
        producer.flush()
        producer.close()

if __name__ == "__main__":
    main()
