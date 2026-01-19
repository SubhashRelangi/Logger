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
        self.file_type = None
        self.compress = False

        self._worker = None
        self._compressor = None

        self._compress_event = multiprocessing.Event()

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

        # WORKER PROCESS 
        self._worker = multiprocessing.Process(
            target=Logger._worker_loop,      
            args=(self, self.q, self.file_type, self.headers_line, self._compress_event),
        )   
        self._worker.start()

        # COMPRESSOR PROCESS
        if self.compress:
            self._compressor = multiprocessing.Process(
                target=Logger._compressor_loop,  
                args=(self, self.file_type, self._compress_event),
            )
            self._compressor.start()

    def publish(self, data):
        record = ",".join(map(str, data)) + "\n"
        try:
            self.q.put(record, timeout=0.01)
        except queue.Full:
            pass  

    def stop(self):
        if self._worker is None:
            return

        # stop worker
        self.q.put(STOP)
        self._worker.join()

        # stop compressor
        if self._compressor:
            self._compress_event.set()
            self._compressor.terminate()
            self._compressor.join()

        self._worker = None
        self._compressor = None


    def _worker_loop(self, q, file_type, headers_line, compress_event):
        file_manager = FileManager(file_type=file_type, compress=False, create_file=True)

        max_bytes = MAX_FILE_SIZE_MB * 1024 * 1024
        current_size = 0

        f = open(file_manager.current_file, "ab")

        if headers_line:
            h = headers_line.encode("utf-8")
            f.write(h)
            current_size += len(h)

        start = time.time()
        sec = 0

        try:
            while True:
                record = q.get()

                if record == STOP:
                    break

                encoded = record.encode("utf-8")
                size = len(encoded)

                if current_size + size >= max_bytes:
                    f.close()

                    # setting compression
                    compress_event.set()

                    file_manager.current_file = file_manager._new_log_file()
                    f = open(file_manager.current_file, "ab")
                    current_size = 0

                    if headers_line:
                        f.write(h)
                        current_size = len(h)

                f.write(encoded)
                current_size += size

                sec += 1
                now = time.time()
                if now - start >= 1.0:
                    print(f"Worker Exc: {sec} | Queue size: {q.qsize()}")
                    sec = 0
                    start = now

        finally:
            f.close()


    def _compressor_loop(self, file_type, compress_event):
        file_manager = FileManager(file_type=file_type, compress=True, create_file=False)

        while True:
            compress_event.wait(timeout=1.0)
            compress_event.clear()

            try:
                file_manager.compress_directory_if_needed()
            except Exception as e:
                print(f"[compressor] error: {e}")
