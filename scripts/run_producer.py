"""
run_producer.py — A simple script to pump demo jobs into the queue.
Run this to give the workers something to do so the Grafana dashboard lights up!
"""

import time
import os
import sys

# Ensure the parent directory (project root) is on the Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from distqueue.client import get_redis_client
from distqueue.producer import enqueue

def main():
    # If running locally outside docker, default to localhost
    os.environ.setdefault("REDIS_HOST", "localhost")
    client = get_redis_client()
    
    print("Starting producer... Press Ctrl+C to stop.")
    count = 0
    try:
        while True:
            # Enqueue a batch of 5 jobs at a time to keep the workers busy
            for _ in range(5):
                enqueue(client, {"task": f"demo-job-{count}"})
                count += 1
            
            print(f"Enqueued {count} jobs so far...")
            # Sleep a bit so we don't overwhelm the system, 
            # but fast enough that 3 workers have plenty to do.
            time.sleep(1) 
    except KeyboardInterrupt:
        print("\nProducer stopped.")

if __name__ == "__main__":
    main()
