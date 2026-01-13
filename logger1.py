import multiprocessing
import queue
import time

from storage import SystemStorage
from file_manager import FileManager
from config import QUEUE_SIZE, MAX_FILE_SIZE_MB

STOP = "__STOP__"


class Logger:
    def __init__(self):
        self.q = multiprocessing.Queue(maxsize=QUEUE_SIZE)
        self.headers_line = None
        self.compress = False
        self.file_type = None
        self._worker = None

    def initialize_logger(self, file_type: str, compress: bool = False):
        if not file_type:
            raise ValueError("file_type must be provided")

        if not SystemStorage().checking():
            raise RuntimeError("Insufficient storage")

        self.file_type = file_type
        self.compress = compress

    def headers(self, *headers):
        if not headers:
            raise ValueError("Headers cannot be empty")
        self.headers_line = ",".join(headers) + "\n"

    def start(self):
        if self._worker is not None:
            raise RuntimeError("Logger already started")

        self._worker = multiprocessing.Process(
            target=_worker_loop,
            args=(
                self.q,
                self.file_type,
                self.compress,
                self.headers_line,
            ),
        )
        self._worker.start()

    def publish(self, data):
        record = ",".join(map(str, data)) + "\n"
        try:
            self.q.put(record, timeout=0.01)
        except queue.Full:
            # Drop log silently (or count drops if needed)
            pass

    def stop(self):
        if self._worker is None:
            return

        # Signal shutdown
        self.q.put(STOP)

        # Wait for clean exit
        self._worker.join()
        self._worker = None


def _worker_loop(q, file_type, compress, headers_line):
    file_manager = FileManager(file_type=file_type, compress=compress)

    max_bytes = MAX_FILE_SIZE_MB * 1024 * 1024
    current_size = 0

    f = open(file_manager.current_file, "a")

    if headers_line:
        h = headers_line
        f.write(h)
        current_size += len(h)

    start = time.time()
    sec = 0

    try:
        while True:
            record = q.get()  # blocking is fine because of STOP sentinel

            if record == STOP:
                break

            encoded = record.encode("utf-8")

            if current_size + len(encoded) >= max_bytes:
                f.close()

                if compress:
                    file_manager.compress_directory_if_needed()

                file_manager.current_file = file_manager._new_log_file()
                f = open(file_manager.current_file, "a")
                current_size = 0

                if headers_line:
                    f.write(h)
                    current_size = len(h)

            f.write(record)
            current_size += len(encoded)
            sec += 1

            now = time.time()
            if now - start >= 1.0:
                print(f"Worker Exc: {sec}")
                sec = 0
                start = now

    finally:
        f.close()
