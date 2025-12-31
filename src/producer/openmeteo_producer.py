#!/home/adm-mcsc/weather_producer/.venv/bin/python
import time
import json
import requests
import yaml
from kafka import KafkaProducer
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DEFAULT_LOCATIONS_PATH = BASE_DIR / "config" / "locations.yaml"

OPENMETEO_URL = "https://api.open-meteo.com/v1/forecast"

def load_locations(path=DEFAULT_LOCATIONS_PATH):
    with open(path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    return cfg["locations"]

def fetch_forecast(lat, lon):
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "wind_speed_10m,wind_gusts_10m,temperature_2m,precipitation,pressure_msl",
        "forecast_days": 2,
        "timeformat": "unixtime",
        "timezone": "UTC",
    }
    r = requests.get(OPENMETEO_URL, params=params, timeout=15)
    r.raise_for_status()
    return r.json()

def iter_hourly_messages(loc_id, lat, lon, data):
    hourly = data.get("hourly", {})
    times = hourly.get("time", [])
    ws = hourly.get("wind_speed_10m", [])
    wg = hourly.get("wind_gusts_10m", [])
    t2m = hourly.get("temperature_2m", [])
    pr = hourly.get("precipitation", [])
    pmsl = hourly.get("pressure_msl", [])

    ingest_time = int(time.time())
    n = min(len(times), len(ws), len(wg), len(t2m), len(pr), len(pmsl))

    for i in range(n):
        yield {
            "location_id": loc_id,
            "latitude": float(lat),
            "longitude": float(lon),
            "forecast_time": int(times[i]),
            "ingest_time": ingest_time,
            "wind_speed_10m": float(ws[i]) if ws[i] is not None else None,
            "wind_gusts_10m": float(wg[i]) if wg[i] is not None else None,
            "temperature_2m": float(t2m[i]) if t2m[i] is not None else None,
            "precipitation": float(pr[i]) if pr[i] is not None else None,
            "pressure_msl": float(pmsl[i]) if pmsl[i] is not None else None,
        }

def main():
    topic = "weather_iceland_raw"

    producer = KafkaProducer(
        bootstrap_servers=["master:9092"],
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        linger_ms=50,
        retries=3,
    )

    locations = load_locations()

    # 每 5 分钟发一次
    while True:
        for loc_id, coords in locations.items():
            lat = coords["latitude"]
            lon = coords["longitude"]
            data = fetch_forecast(lat, lon)

            # 先发 24 条（24小时），避免一口气太多
            for j, msg in enumerate(iter_hourly_messages(loc_id, lat, lon, data)):
                if j >= 24:
                    break
                producer.send(topic, msg)

            print(f"[OK] sent 24 records for {loc_id}")

        producer.flush()
        print("[OK] batch done, sleep 300s")
        time.sleep(300)

if __name__ == "__main__":
    main()
