import threading
import queue
import time
import struct, json
from openpyxl import Workbook
from config import QUEUE_SIZE, MAX_FILE_SIZE_MB, DEFAULT_FILE_TYPE, DEFAULT_COMPRESS, ENCODER
from storage import SystemStorage
from file_manager import FileManager

class Logger:
    def __init__(self):
        try:
            self.file_manager = None
            self._running = False
            self.file_type = None
            self.schema = None

            self.q = queue.Queue(maxsize=QUEUE_SIZE)
            self.headers_blob = None
            self.dropped_count = 0


            self._worker = None
            self._compressor = None

            self._compress_event = threading.Event()

        except Exception as e:
            print(f"Exception in init: {e}")

    
    # =========================== INITILIZER ========================
    def initialize(self, file_type: str = DEFAULT_FILE_TYPE, compress: bool = DEFAULT_COMPRESS):
        try:
            if not file_type:
                raise ValueError("file_type must be provided")

            if not SystemStorage().checking():
                raise RuntimeError("Insufficient storage")
            
            self.file_type = file_type
            self.file_manager = FileManager(file_type=file_type, compress=compress)
        
        except Exception as e:
            print(f"Exception in initilizer: {e}")


    # =========================== START ========================
    def start(self):
        try:
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

                case "xlsx":
                    self._worker = threading.Thread(
                        target=self.xlsx_worker,
                        daemon=True
                    )
                case _:
                    raise ValueError("Invalied file format.")
                
            self._worker.start()
                
            if self.file_manager.compress:
                self._compressor = threading.Thread(
                    target=self._compressor_loop,
                    daemon=True
                )
                self._compressor.start()

        except Exception as e:
            print(f"Exception in start: {e}")

    # =========================== HEADER WRITER ========================
    def headers(self, *headers):
        try:
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

                case "xlsx":
                    self.headers_blob = self.schema

                case _:
                    raise ValueError(f"Unsupported file type: {self.file_type}")
                
        except Exception as e:
            print(f"Exception in header: {e}")

        

    # =========================== PUBLISHER ========================
    def publish(self, values = None, encode = ENCODER):

        try:

            if values is None:
                raise ValueError("Values cannot be None")

            
            if self.file_type in ("bin", "tlv.bin"):

                # encode = False → MUST be bytes
                if not encode:
                    if not isinstance(values, (bytes, bytearray)):
                        raise TypeError(
                            "Values are not binary. Set encode=True to encode them."
                        )
                    record = values

                # encode = True → MUST be structured
                else:
                    if isinstance(values, (bytes, bytearray)):
                        raise TypeError(
                            "Values are already binary. Set encode=False."
                        )

                    if not isinstance(values, (list, tuple)):
                        raise TypeError(
                            "Binary encoder expects list or tuple values."
                        )

                    if self.file_type == "bin":
                        record = self._encode_record_bin(values)
                    else:
                        record = self._encode_record_tlvbin(values)

            elif self.file_type in ("csv", "xlxs"):
                if not isinstance(values, (list, tuple)):
                    raise TypeError("XLSX logger expects list or tuple")
                record = values

            else:
                raise ValueError(f"Unsupported file type: {self.file_type}")

            # ---------- QUEUE ----------
            try:
                self.q.put(record, timeout=0.01)
            except queue.Full:
                self.dropped_count += 1


        except Exception as e:
            print(f"Exception in publish: {e}")

    
    # =========================== STOP ========================
    def stop(self):
        try:
            self._running = False
            self._compress_event.set()  
            self._worker.join()
            if self._compressor:
                self._compressor.join()
        except Exception as e:
            print(f"Exception in stop: {e}")
    
    # =========================== BIN ENCODER ========================
    def _encode_record_bin(self, values):
        try:
            payload = bytearray()

            for value in values:

                if isinstance(value, bool):
                    payload += struct.pack("<?", value)

                elif isinstance(value, int):
                    payload += struct.pack("<q", value)   # int64

                elif isinstance(value, float):
                    payload += struct.pack("<d", value)   # float64

                elif isinstance(value, str):
                    payload += value.encode("utf-8") + b"\x00"  

                elif isinstance(value, bytes):
                    payload += value  # raw bytes (FIXED SIZE REQUIRED)

                else:
                    raise TypeError(f"Unsupported type: {type(value)}")

            return bytes(payload)
    
        except Exception as e:
            print(f"Exception in binary encoder: {e}")
    
    # # =========================== TLV BIN ENCODER ========================
    def _encode_record_tlvbin(self, values):
        try:
            if not self.schema:
                raise RuntimeError("Schema not set. Call headers() first.")

            if len(values) != len(self.schema):
                raise ValueError("Record does not match schema length")
            
            TYPE_BOOL   = 1
            TYPE_INT    = 2
            TYPE_FLOAT  = 3
            TYPE_STRING = 4
            TYPE_BYTES  = 5
            TYPE_NONE   = 6

            buf = bytearray()

            for value in values:

                # ---------- TYPE + VALUE ----------
                if value is None:
                    buf += TYPE_NONE.to_bytes(1, "little")
                    buf += (0).to_bytes(2, "little")

                elif isinstance(value, bool):
                    buf += TYPE_BOOL.to_bytes(1, "little")
                    buf += (1).to_bytes(2, "little")
                    buf += b"\x01" if value else b"\x00"

                elif isinstance(value, int):
                    data = struct.pack("<q", value)  # int64
                    buf += TYPE_INT.to_bytes(1, "little")
                    buf += len(data).to_bytes(2, "little")
                    buf += data

                elif isinstance(value, float):
                    data = struct.pack("<d", value)  # float64
                    buf += TYPE_FLOAT.to_bytes(1, "little")
                    buf += len(data).to_bytes(2, "little")
                    buf += data 

                elif isinstance(value, str):
                    data = value.encode("utf-8")
                    buf += TYPE_STRING.to_bytes(1, "little")
                    buf += len(data).to_bytes(2, "little")
                    buf += data

                elif isinstance(value, bytes):
                    buf += TYPE_BYTES.to_bytes(1, "little")
                    buf += len(value).to_bytes(2, "little")
                    buf += value

                else:
                    raise TypeError(f"Unsupported type: {type(value)}")

            record = bytearray()
            record += len(buf).to_bytes(2, "little")
            record += buf

            return bytes(record)

        except Exception as e:
            raise RuntimeError(f"Exception in TLV encoder: {e}")


        
    # =========================== BIN WORKER ========================
    def bin_worker(self):
        try:
            max_bytes = MAX_FILE_SIZE_MB * 1024 * 1024
            current_size = 0
            start = time.time()
            sec_count = 0

            # open first file
            f = open(self.file_manager.current_file, "ab")

            # WRITE HEADER 
            f.write(self.headers_blob)
            f.flush()

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
                        f.flush()

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
        except Exception as e:
            print(f"Exception in Bin Worker: {e}")

    # =========================== TLV BIN WORKER ========================
    def tlv_worker(self):
        try:
            max_bytes = MAX_FILE_SIZE_MB * 1024 * 1024
            current_size = 0
            start = time.time()
            sec_count = 0

            f = open(self.file_manager.current_file, "ab")

            f.write(self.headers_blob)
            f.flush()

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
                        f.flush()

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
        except Exception as e:
            print(f"Exception in TLV Bin Worker: {e}")


    # =========================== CSV WORKER ========================
    def csv_worker(self):
        try:
            max_bytes = MAX_FILE_SIZE_MB * 1024 * 1024
            current_size = 0

            f = open(self.file_manager.current_file, "ab")

            if self.headers_blob:
                f.write(self.headers_blob)
                f.flush()
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
                            f.flush()
                            current_size += len(self.headers_blob)

                    f.write(encoded)
                    current_size += size
                    self.q.task_done()

            finally:
                f.close()
        except Exception as e:
            print(f"Exception in CSV worker: {e}")


    def xlsx_worker(self):
        try:
            # Excel hard limit ≈ 1,048,576
            MAX_ROWS = 250_000
            wb = Workbook(write_only=True)
            ws = wb.create_sheet(title="log")
            row_count = 0

            def prepare_new_sheet(workbook):
                sheet = workbook.create_sheet(title="log")
                count = 0
                if self.schema:
                    sheet.append(list(self.schema))
                    count = 1
                return sheet, count

            # Initial header setup
            if self.schema:
                ws.append(list(self.schema))
                row_count = 1

            try:
                while self._running or not self.q.empty():
                    try:
                        # Use a slightly longer timeout to reduce CPU spikes
                        record = self.q.get(timeout=0.5)
                    except queue.Empty:
                        continue

                    try:
                        ws.append(list(record))
                        row_count += 1
                    finally:
                        # Always mark task done even if append fails
                        self.q.task_done()

                    # Rotate XLSX file
                    if row_count >= MAX_ROWS:
                        wb.save(self.file_manager.current_file)
                        self._compress_event.set()

                        # Setup new workbook
                        self.file_manager.current_file = self.file_manager._new_log_file()
                        wb = Workbook(write_only=True)
                        ws, row_count = prepare_new_sheet(wb)

            except Exception as e:
                # Log your error here so the thread doesn't die silently
                print(f"Worker error: {e}")
            finally:
                # Only save if we actually wrote data beyond the header
                # or if the file doesn't exist yet.
                try:
                    wb.save(self.file_manager.current_file)
                    wb.close()
                except Exception:
                    pass

        except Exception as e:
            print(f"Exception in XLSX Worker: {e}")

    
    # =========================== COMPRESSOR LOOP ========================
    def _compressor_loop(self):
            try:
                while self._running:
                    if self._compress_event.wait(timeout=1.0):
                        self._compress_event.clear()

                    if not self._running:
                        break

                    try:
                        self.file_manager.compress_directory_if_needed()
                    except Exception as e:
                        print(f"[compressor] error: {e}")

            except Exception as e:
                print(f"Exception in compressor loop: {e}")