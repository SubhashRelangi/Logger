from logger import Logger
import time
from datetime import datetime

def time_value():
    now = datetime.now()
    timestamp = now.strftime("%H:%M:%S:%f")[:-3]
    return timestamp

def main():

    logger = Logger()
    logger.initialize("csv")
    logger.headers("timestamp", "payload1", "payload2")
    logger.start()

    start = time.perf_counter()
    sec_count = 0
    record = [time_value(), "xxxx", "yyyyy"]

    while True:
        end = time.perf_counter()
        sec_count += 1

        logger.publish(record)

        if end - start >= 1.0:
            print(f"[Main] exc -> {sec_count}")
            sec_count = 0
            start = end

if __name__ == "__main__":
    main()