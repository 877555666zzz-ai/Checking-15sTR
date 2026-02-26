import time
from summary_sync import run_summary_once

while True:
    try:
        run_summary_once()
    except Exception as e:
        print("[ERROR] run failed:", e)
    time.sleep(15)