# LoggerModule

## Overview

This project is a high-performance, asynchronous data logger designed for high-frequency data collection applications. It is built to be robust, ensuring that data logging does not block the main application thread and handles large volumes of data efficiently.

The logger uses a producer-consumer pattern to decouple data collection from file I/O operations. Data is published to a large in-memory queue and a dedicated worker thread is responsible for writing the data to disk.

## Features

*   **Asynchronous Logging:** Data is published to an in-memory queue and written to disk by a background thread, preventing blocking calls in the main application.
*   **High-Frequency Data Handling:** Optimized for scenarios where data is generated at a high rate.
*   **Log Rotation:** Automatically rotates log files when they reach a configurable size limit (`MAX_FILE_SIZE_MB`).
*   **Automatic Compression:** Compresses the log directory when the total size of log files exceeds a specified limit, helping to manage disk space.
*   **Disk Space Monitoring:** Checks for available disk space before starting to prevent the application from filling up the storage.

## Architecture

The logger is composed of several key components:

*   `main.py`: The entry point of the application, which demonstrates how to instantiate and use the `Logger`.
*   `logger.py`: Contains the core `Logger` class. It manages the producer-consumer queue and the background worker thread for file writing.
*   `file_manager.py`: Handles all file system operations, including log file creation, rotation, and directory compression.
*   `storage.py`: A utility to check the available system storage to ensure there is enough space for logging.
*   `config.py`: A centralized configuration file for all tunable parameters, such as queue size, file size limits, and storage thresholds.

## Configuration

All logger parameters can be adjusted in `config.py`:

*   `QUEUE_SIZE`: The maximum number of items to buffer in the in-memory queue.
*   `MAX_FILE_SIZE_MB`: The maximum size (in MB) a log file can reach before it is rotated.
*   `LOG_DIRECTORY`: The directory where log files will be stored.
*   `STORAGE_THRESHOLD_PERCENT`: The minimum percentage of free disk space required to start the logger.
*   `MAX_DIRECTORY_SIZE_MB`: The total size (in MB) the log directory can reach before compression is triggered.

## Usage

To use the logger, import the `Logger` class and instantiate it. Then, call the `publish` method to log data.

```python
import time
from logger import Logger

def main():
    # Initialize the logger
    # Set compress=True to enable directory compression
    logger = Logger(compress=True)
    logger.start()

    print("Logger started. Publishing data...")

    try:
        # Example of publishing data in a loop
        for i in range(1000):
            data_packet = f"Data packet {i}"
            logger.publish(data_packet)
            time.sleep(0.01)  # Simulate high-frequency data
    except KeyboardInterrupt:
        print("Stopping logger...")
    finally:
        logger.stop()
        print("Logger stopped.")

if __name__ == "__main__":
    main()
```
