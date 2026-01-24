from logger import Logger
import time
from datetime import datetime

def time_value():
    now = datetime.now()
    timestamp = now.strftime("%H:%M:%S:%f")[:-3]
    return timestamp

def main():

    logger = Logger()
    logger.initialize("bin")
    logger.headers("timestamp", "payload1", "payload2")
    logger.start()

    start = time.perf_counter()
    sec_count = 0
    record = [time.time(), 1000, 1.11110]

    # record = bytes.fromhex(
    #     "1B 00 "
    #     "0C 00 31 32 3A 33 34 3A 35 36 3A 37 38 39 "
    #     "04 00 78 78 78 78 "
    #     "05 00 79 79 79 79 79"
    # )

    while True:
        end = time.perf_counter()
        sec_count += 1

        logger.publish(record, encode=True)

        if end - start >= 1.0:
            print(f"[Main] exc -> {sec_count}")
            sec_count = 0
            start = end

if __name__ == "__main__":
    main()