import os
import time
from summary_sync import run_summary_once

LOOP_SLEEP_SEC = float(os.getenv("LOOP_SLEEP_SEC", "3"))

def main():
    while True:
        try:
            run_summary_once()
        except Exception as e:
            print(f"[ERROR] {type(e).__name__}: {e}")
        time.sleep(LOOP_SLEEP_SEC)

if __name__ == "__main__":
    main()