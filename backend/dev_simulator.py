# backend/dev_simulator.py
import requests
import random
import time
import datetime as dt

URL = "http://127.0.0.1:8000/ingest"  # change if backend runs elsewhere
stations = ["STN_001", "STN_002", "STN_003"]

def gen_reading():
    # produce plausible skewed rainfall series
    r1 = max(0.0, random.gauss(4.0, 6.0))
    r24 = max(r1, random.gauss(35.0, 22.0))
    r72 = max(r24, random.gauss(100.0, 60.0))
    return round(r1,2), round(r24,2), round(r72,2)

if __name__ == "__main__":
    while True:
        for s in stations:
            now = dt.datetime.utcnow().isoformat() + "Z"
            r1, r24, r72 = gen_reading()
            payload = {
                "station_id": s,
                "timestamp": now,
                "rainfall_mm_1h": r1,
                "rainfall_mm_24h": r24,
                "rainfall_mm_72h": r72
            }
            try:
                res = requests.post(URL, json=payload, timeout=5)
                print(f"[{now}] POST {s} -> {res.status_code}")
            except Exception as e:
                print("POST failed:", e)
        time.sleep(5)
