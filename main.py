import time
import datetime
from logger import Logger

def realtime_hms_ms():
    now = datetime.now()
    return now.strftime("%H:%M:%S") + f":{now.microsecond // 1000:03d}"

def main():
    
    logger = Logger()

    logger.initialize_logger("csv", compress=True)

    logger.headers("timestamp","degrees0", "degrees1","volts0","amps0","watts0","volts1","amps1","watts1","satellite_count","heading","latitude", "longitude")

    logger.start()

    while logger._running:
        logger.publish([realtime_hms_ms," 195.029296875", "314.12109375", "24.0538711547852", "-0.014181817881763", "-0.238541349768639", "-0.006679157260805", "0.018545454367995", "-0.079513780772686", "9", "233.245483398438", "22.5792655944824", "75.7095489501953"])
        time.sleep(0.1)