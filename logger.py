import threading
import queue
import time
from openpyxl import Workbook
from config import QUEUE_SIZE, MAX_FILE_SIZE_MB
from storage import SystemStorage
from file_manager import FileManager

class Logger:
    def __init__(self):
        self.file_manager = None
        self._running = False
        self.file_type = None
        self.schema = None

        self.q = queue.Queue(maxsize=QUEUE_SIZE)
        self.headers_blob = None

        self._worker = None
        self._compressor = None

        self._compress_event = threading.Event()

    
    # =========================== INITILIZER ========================
    def initialize(self, file_type: str, compress: bool = False):
        if not file_type:
            raise ValueError("file_type must be provided")

        if not SystemStorage().checking():
            raise RuntimeError("Insufficient storage")
        
        self.file_type = file_type
        self.file_manager = FileManager(file_type=file_type, compress=compress)


    # =========================== START ========================
    def start(self):
        if self.file_manager is None:
            raise RuntimeError("Logger not initialized")
        
        self._running = True

        match self.file_type:
            case "csv":
                self._worker = threading.Thread(
                    target=self.csv_worker,
                    daemon=True
                )

            case "bin":
                self._worker = threading.Thread(
                    target=self.bin_worker,
                    daemon=True
                )

            case "tlv.bin":
                self._worker = threading.Thread(
                    target=self.tlv_worker,
                    daemon=True
                )

            # case "xlsx":
            #     self._worker = threading.Thread(
            #         target=self.xlsx_worker,
            #         daemon=True
            #     )
            case _:
                raise ValueError("Invalied file format.")
            
        self._worker.start()
            
        if self.file_manager.compress:
            self._compressor = threading.Thread(
                target=self._compressor_loop,
                daemon=True
            )
            self._compressor.start()

    # =========================== HEADER WRITER ========================
    def headers(self, *headers):
        if not headers:
            raise ValueError("Headers cannot be empty")

        # Store schema once (format-agnostic)
        self.schema = tuple(headers)

        match self.file_type:
            case "csv":
                self.headers_blob = (
                    ",".join(self.schema) + "\n"
                ).encode("utf-8")

            case "bin":
                buf = bytearray()
                buf += b"LOG1"                     # magic
                buf += (1).to_bytes(1, "little")   # version
                buf += len(self.schema).to_bytes(1, "little")

                for name in self.schema:
                    b = name.encode("utf-8")
                    buf += len(b).to_bytes(1, "little")
                    buf += b

                self.headers_blob = bytes(buf)

            case "tlv.bin":
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

            # case "xlsx":
            #     self.headers_blob = self.schema

            case _:
                raise ValueError(f"Unsupported file type: {self.file_type}")

        

    # =========================== PUBLISHER ========================
    def publish(self, values):
        
        # match self.file_type:
        #     case "bin":
        #         record = self._encode_record_bin(values)

        #     case "tlv.bin":
        #         record = self._encode_record_tlvbin(values)

        #     case _:
        #         record = values

        try:
            self.q.put(values, timeout=0.01)
        except queue.Full:
            pass

    
    # =========================== STOP ========================
    def stop(self):
        self._running = False
        self._compress_event.set()  
        self._worker.join()
        if self._compressor:
            self._compressor.join()
    
    # =========================== BIN ENCODER ========================
    # def _encode_record_bin(self, values):
    #     if not self.schema:
    #         raise RuntimeError("Schema not set. Call headers() first.")

    #     if len(values) != len(self.schema):
    #         raise ValueError("Record does not match schema length")

    #     payload = bytearray()

    #     for value in values:
    #         b = str(value).encode("utf-8")
    #         payload += len(b).to_bytes(2, "little")
    #         payload += b

    #     record = bytearray()
    #     record += len(payload).to_bytes(2, "little")
    #     record += payload

    #     return bytes(record)
    
    # # =========================== TLV BIN ENCODER ========================
    # def _encode_record_tlvbin(self, values):
    #     if not self.schema:
    #         raise RuntimeError("Schema not set. Call headers() first.")

    #     if len(values) != len(self.schema):
    #         raise ValueError("Record does not match schema length")
        
    #     buf = bytearray()

    #     for field_id, value in enumerate(values):
    #         v = str(value).encode("utf-8")

    #         buf += field_id.to_bytes(1, "little")   # Type
    #         buf += len(v).to_bytes(2, "little")     # Length
    #         buf += v                                # Value

    #     record = bytearray()
    #     record += len(buf).to_bytes(2, "little")   # record length
    #     record += buf

    #     return bytes(record)
        
    # =========================== BIN WORKER ========================
    def bin_worker(self):
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

                if current_size + size > max_bytes:
                    f.close()
                    self._compress_event.set()

                    self.file_manager.current_file = self.file_manager._new_log_file()
                    f = open(self.file_manager.current_file, "ab")

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

    # =========================== TLV BIN WORKER ========================
    def tlv_worker(self):
        max_bytes = MAX_FILE_SIZE_MB * 1024 * 1024
        current_size = 0
        start = time.time()
        sec_count = 0

        f = open(self.file_manager.current_file, "ab")

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

                if current_size + size > max_bytes:
                    f.close()
                    self._compress_event.set()

                    self.file_manager.current_file = self.file_manager._new_log_file()
                    f = open(self.file_manager.current_file, "ab")

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

    # =========================== CSV WORKER ========================
    def csv_worker(self):
        max_bytes = MAX_FILE_SIZE_MB * 1024 * 1024
        current_size = 0

        f = open(self.file_manager.current_file, "ab")

        if self.headers_blob:
            f.write(self.headers_blob)
            current_size += len(self.headers_blob)

        try:
            while self._running or not self.q.empty():
                try:
                    record = self.q.get(timeout=0.1)
                except queue.Empty:
                    continue

                line = ",".join(map(str, record)) + "\n"

                encoded = line.encode("utf-8")
                size = len(encoded)

                if current_size + size >= max_bytes:
                    f.close()
                    self._compress_event.set()

                    self.file_manager.current_file = self.file_manager._new_log_file()
                    f = open(self.file_manager.current_file, "ab")
                    current_size = 0

                    if self.headers_blob:
                        f.write(self.headers_blob)
                        current_size += len(self.headers_blob)

                f.write(encoded)
                current_size += size
                self.q.task_done()

        finally:
            f.close()


    # def xlsx_worker(self):
    #     # Excel hard limit ≈ 1,048,576
    #     MAX_ROWS = 1_000_000
    #     wb = Workbook(write_only=True)
    #     ws = wb.create_sheet(title="log")
    #     row_count = 0

    #     def prepare_new_sheet(workbook):
    #         sheet = workbook.create_sheet(title="log")
    #         count = 0
    #         if self.schema:
    #             sheet.append(list(self.schema))
    #             count = 1
    #         return sheet, count

    #     # Initial header setup
    #     if self.schema:
    #         ws.append(list(self.schema))
    #         row_count = 1

    #     try:
    #         while self._running or not self.q.empty():
    #             try:
    #                 # Use a slightly longer timeout to reduce CPU spikes
    #                 record = self.q.get(timeout=0.5)
    #             except queue.Empty:
    #                 continue

    #             try:
    #                 ws.append(list(record))
    #                 row_count += 1
    #             finally:
    #                 # Always mark task done even if append fails
    #                 self.q.task_done()

    #             # Rotate XLSX file
    #             if row_count >= MAX_ROWS:
    #                 wb.save(self.file_manager.current_file)
    #                 self._compress_event.set()

    #                 # Setup new workbook
    #                 self.file_manager.current_file = self.file_manager._new_log_file()
    #                 wb = Workbook(write_only=True)
    #                 ws, row_count = prepare_new_sheet(wb)

    #     except Exception as e:
    #         # Log your error here so the thread doesn't die silently
    #         print(f"Worker error: {e}")
    #     finally:
    #         # Only save if we actually wrote data beyond the header
    #         # or if the file doesn't exist yet.
    #         try:
    #             wb.save(self.file_manager.current_file)
    #             wb.close()
    #         except Exception:
    #             pass

    
    # =========================== COMPRESSOR LOOP ========================
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