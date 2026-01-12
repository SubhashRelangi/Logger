import threading
import queue
from storage import SystemStorage
from file_manager import FileManager
from config import QUEUE_SIZE, MAX_FILE_SIZE_MB

class Logger:

    def __init__(self):
        self.file_manager = None
        self._running = False
        self.q = queue.Queue(maxsize=QUEUE_SIZE)
        self.headers_line = None
        self._worker = None

    def initialize_logger(self, file_type: str, compress: bool = False):
        if not file_type:
            raise ValueError("file_type must be provided")

        if not SystemStorage().checking():
            raise RuntimeError("Insufficient storage")

        self.file_manager = FileManager(file_type=file_type, compress=compress)

    def start(self):
        if self.file_manager is None:
            raise RuntimeError("Logger not initialized")

        self._running = True
        self._worker = threading.Thread(
            target=self._worker_loop,
            daemon=True
        )
        self._worker.start()

    def headers(self, *headers):
        if not headers:
            raise ValueError("Headers cannot be empty")

        # store once, worker will write per file
        self.headers_line = ",".join(headers) + "\n"

    def publish(self, data):
        record = ",".join(map(str, data)) + "\n"
        try:
            self.q.put(record)
        except queue.Full:
            pass

    def stop(self):
        self._running = False
        self._worker.join()

    def _worker_loop(self):
        max_bytes = MAX_FILE_SIZE_MB * 1024 * 1024
        current_size = 0

        f = open(self.file_manager.current_file, "ab")

        if self.headers_line:
            h = self.headers_line.encode("utf-8")
            f.write(h)
            current_size += len(h)

        try:
            while self._running or not self.q.empty():
                try:
                    record = self.q.get(timeout=0.1)
                except queue.Empty:
                    continue

                encoded = record.encode("utf-8")

                if current_size + len(encoded) >= max_bytes:
                    f.close()
                    self.file_manager.compress_directory_if_needed()
                    self.file_manager.current_file = self.file_manager._new_log_file()
                    f = open(self.file_manager.current_file, "ab")
                    current_size = 0

                    if self.headers_line:
                        h = self.headers_line.encode("utf-8")
                        f.write(h)
                        current_size += len(h)

                f.write(encoded)
                current_size += len(encoded)
                self.q.task_done()
        finally:
            f.close()
