import threading
import queue
from storage import SystemStorage
from file_manager import FileManager
from config import QUEUE_SIZE

class Logger:

    def __init__(self):
        self.file_manager = None
        self._running = False
        self.q = queue.Queue(maxsize=QUEUE_SIZE)
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
        if not self.file_manager:
            print("logger not initialized")

        if not headers:
            print("Headers cannot be empty")

        self.headers_list = headers
        with open(self.file_manager.current_file, "a") as f:
            f.write(",".join(headers) + "\n")


    def publish(self, data):
        record = ",".join(map(str, data)) + "\n"
        try:
            self.q.put(record, block=False)
        except queue.Full:
            pass


    def stop(self):
        self._running = False


    def _worker_loop(self):
        with open(self.file_manager.current_file, "a", encoding="utf-8") as f:
            while self._running or not self.q.empty():
                try:
                    record = self.q.get()
                except queue.Empty:
                    continue

                self.file_manager.rotate_if_needed()
                f.write(record)
                self.q.task_done()
