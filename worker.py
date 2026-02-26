import time
from summary_sync import run_summary_once

while True:
    run_summary_once()
    time.sleep(15)