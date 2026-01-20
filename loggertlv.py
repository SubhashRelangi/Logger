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
        self.schema = None
        self.headers_blob = None

        self._worker = None
        self._compressor = None
        self._compress_event = threading.Event()

    def initilizer(self, file_type, compress):
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
                    target=self.tlv_worker,
                    daemon=True
                )
        
        self._worker.start()
            
        if self.file_manager.compress:
            self._compressor = threading.Thread(
                target=self._compressor_loop,
                daemon=True
            )
            self._compressor.start()

    def headers(self, *headers):
        self.schema = tuple(headers)

        buf = bytearray()
        buf += b"TLV1"                     # magic
        buf += (1).to_bytes(1, "little")   # version
        buf += len(self.schema).to_bytes(1, "little")

        FIELD_DEF = 0x01
        for name in self.schema:
            b = name.encode("utf-8")
            buf += FIELD_DEF.to_bytes(1, "little")
            buf += len(b).to_bytes(2, "little")
            buf += b

        self.headers_blob = bytes(buf)

    def publish(self, values):
        record = self._encode_record(values)
        try:
            self.q.put(record, timeout=0.01)
        except queue.Full:
            pass

    def _encode_record(self, values):
        if not self.schema:
            raise RuntimeError("Schema not set. Call headers() first.")

        if len(values) != len(self.schema):
            raise ValueError("Record does not match schema length")
        
        buf = bytearray()

        for field_id, value in enumerate(values):
            v = str(value).encode("utf-8")

            buf += field_id.to_bytes(1, "little")   # Type
            buf += len(v).to_bytes(2, "little")     # Length
            buf += v                                # Value

        record = bytearray()
        record += len(buf).to_bytes(2, "little")   # record length
        record += buf

        return bytes(record)

    def stop(self):
        self._running = False
        self._compress_event.set()

        if self._worker:
            self._worker.join()

        if self._compressor:
            self._compressor.join()

    def tlv_worker(self):
        max_bytes = MAX_FILE_SIZE_MB * 1024 * 1024
        current_size = 0
        start = time.time()
        sec_count = 0

        # open first file
        f = open(self.file_manager.current_file, "ab")

        # WRITE HEADER 
        f.write(self.headers_blob)
        current_size = len(self.headers_blob)

        try:
            while self._running or not self.q.empty():
                end = time.time()
                try:
                    record = self.q.get(timeout=0.1)
                except queue.Empty:
                    continue

                size = len(record)

                # rotate BEFORE write
                if current_size + size > max_bytes:
                    f.close()
                    self._compress_event.set()

                    self.file_manager.current_file = self.file_manager._new_log_file()
                    f = open(self.file_manager.current_file, "ab")

                    # write header again
                    f.write(self.headers_blob)
                    current_size = len(self.headers_blob)

                f.write(record)
                current_size += size
                sec_count += 1

                if end - start >= 1.0:
                    print(f"[Worker] Exc: {sec_count}")
                    sec_count = 0
                    start = end

                self.q.task_done()

        finally:
            f.close()


    def _compressor_loop(self):
        while self._running:
            self._compress_event.wait(timeout=1.0)
            self._compress_event.clear()

            if not self._running:
                break

            try:
                self.file_manager.compress_directory_if_needed()
            except Exception as e:
                print(f"[compressor] error: {e}")

