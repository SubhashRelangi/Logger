import threading
import queue
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

    def initilizer(self, file_type: str, compress: bool = False):
        if not file_type:
            raise ValueError("file_type must be provided")

        if not SystemStorage().checking():
            raise RuntimeError("Insufficient storage")
        
        self.file_type = file_type
        self.file_manager = FileManager(file_type=file_type, compress=compress)


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

            case "tlvbin":
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

    def headers(self, *headers):
        if not headers:
            raise ValueError("Headers cannot be empty")

        # Store schema once (format-agnostic)
        self.schema = tuple(headers)

        match self.file_type:
            case "csv":
                # CSV → header is a text row
                self.headers_blob = (
                    ",".join(self.schema) + "\n"
                ).encode("utf-8")

            case "bin":
                # BIN → binary schema header
                buf = bytearray()
                buf += b"LOG1"                     # magic
                buf += (1).to_bytes(1, "little")   # version
                buf += len(self.schema).to_bytes(1, "little")

                for name in self.schema:
                    b = name.encode("utf-8")
                    buf += len(b).to_bytes(1, "little")
                    buf += b

                self.headers_blob = bytes(buf)

            case "tlv":
                # TLV → schema encoded as FIELD_DEF TLVs
                FIELD_DEF = 0x01
                buf = bytearray()

                for name in self.schema:
                    b = name.encode("utf-8")
                    buf += FIELD_DEF.to_bytes(1, "little")      # type
                    buf += len(b).to_bytes(2, "little")         # length
                    buf += b                                    # value

                self.headers_blob = bytes(buf)

            case "xlsx":
                # XLSX → no binary header, handled by worksheet
                self.headers_blob = self.schema

            case _:
                raise ValueError(f"Unsupported file type: {self.file_type}")

        

    def publish(self, record):
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

    # def bin_worker(self): # needs to change in the conversion of the format to store
    #     max_bytes = MAX_FILE_SIZE_MB * 1024 * 1024
    #     current_size = 0

    #     f = open(self.file_manager.current_file, "ab")

    #     if self.headers_blob:
    #         h = self.headers_blob
    #         f.write(h)
    #         current_size += len(h)

    #     try:
    #         while self._running or not self.q.empty():
    #             try:
    #                 record = self.q.get(timeout=0.1)
    #             except queue.Empty:
    #                 continue

    #             encoded = record.encode("utf-8")
    #             size = len(encoded)

    #             if current_size + size >= max_bytes:
    #                 f.close()

    #                 self._compress_event.set()
                    
    #                 self.file_manager.current_file = self.file_manager._new_log_file()
    #                 f = open(self.file_manager.current_file, "ab")
    #                 current_size = 0

    #                 if self.headers_line:
    #                     h = self.headers_blob
    #                     f.write(h)
    #                     current_size += len(h)

    #             f.write(encoded)
    #             current_size += size
    #             self.q.task_done()

    #     finally:
    #         f.close()


    # def tlv_worker(self):
    #     pass

    def csv_worker(self):
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
                size = len(encoded)

                if current_size + size >= max_bytes:
                    f.close()

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
                self.q.task_done()

        finally:
            f.close()

    def xlsx_worker(self):
        # Excel hard limit ≈ 1,048,576
        MAX_ROWS = 1_000_000
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