import threading
import queue
import time
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
        self._compressor = None

        # signal for compression
        self._compress_event = threading.Event()

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

        # start compressor ONLY if enabled
        if self.file_manager.compress:
            self._compressor = threading.Thread(
                target=self._compressor_loop,
                daemon=True
            )
            self._compressor.start()

    def headers(self, *headers):
        if not headers:
            raise ValueError("Headers cannot be empty")
        self.headers_line = ",".join(headers) + "\n"

    def publish(self, data):
        record = ",".join(map(str, data)) + "\n"
        try:
            self.q.put(record, timeout=0.01)
        except queue.Full:
            pass  

    def stop(self):
        self._running = False
        self._compress_event.set()  
        self._worker.join()
        if self._compressor:
            self._compressor.join()

    # ---------------- WORKER ----------------
    def _worker_loop(self):
        max_bytes = MAX_FILE_SIZE_MB * 1024 * 1024
        current_size = 0

        f = open(self.file_manager.current_file, "ab")

        if self.headers_line:
            h = self.headers_line.encode("utf-8")
            f.write(h)
            current_size += len(h)

        start = time.time()
        sec = 0

        try:
            while self._running or not self.q.empty():
                try:
                    pref = time.time()
                    record = self.q.get(timeout=0.1)
                except queue.Empty:
                    continue

                encoded = record.encode("utf-8")
                size = len(encoded)

                # ---- rotation  ----
                if current_size + size >= max_bytes:
                    f.close()

                    # signal compressor 
                    self._compress_event.set()

                    self.file_manager.current_file = self.file_manager._new_log_file()
                    f = open(self.file_manager.current_file, "ab")
                    current_size = 0

                    if self.headers_line:
                        h = self.headers_line.encode("utf-8")
                        f.write(h)
                        current_size += len(h)

                f.write(encoded)
                current_size += size

                sec += 1
                if pref - start >= 1.0:
                    print(f"Worker Exc: {sec} | Queue size: {self.q.qsize()}")
                    sec = 0
                    start = pref

                self.q.task_done()

        finally:
            f.close()

    # ---------------- COMPRESSOR ----------------
    def _compressor_loop(self):
        """
        Runs in background.
        NEVER blocks the writer.
        """
        while self._running:
            self._compress_event.wait(timeout=1.0)
            self._compress_event.clear()

            if not self._running:
                break

            try:
                self.file_manager.compress_directory_if_needed()
            except Exception as e:
                print(f"[compressor] error: {e}")
