import threading
import queue
import time
from openpyxl import Workbook
from config import QUEUE_SIZE
from storage import SystemStorage
from file_managerxlsx import FileManager


class Logger:
    def __init__(self):
        self.file_manager = None
        self.file_type = None
        self.headers_list = None

        self.q = queue.Queue(maxsize=QUEUE_SIZE)

        self._running = False
        self._worker_thread = None
        self._compressor_thread = None
        self._compress_event = threading.Event()

    # ---------- INIT ----------
    def initilizer(self, file_type: str, compress: bool = False):
        if not file_type:
            raise ValueError("file_type must be provided")

        if not SystemStorage().checking():
            raise RuntimeError("Insufficient storage")

        self.file_type = file_type
        self.file_manager = FileManager(file_type=file_type, compress=compress)

    # ---------- HEADERS ----------
    def headers(self, *headers):
        if not headers:
            raise ValueError("Headers cannot be empty")
        self.headers_list = tuple(headers)

    # ---------- START ----------
    def start(self):
        if self.file_manager is None:
            raise RuntimeError("Logger not initialized")

        self._running = True

        self._worker_thread = threading.Thread(
            target=self._xlsx_worker_loop,
            daemon=False
        )
        self._worker_thread.start()

        if self.file_manager.compress:
            self._compressor_thread = threading.Thread(
                target=self._compressor_loop,
                daemon=True
            )
            self._compressor_thread.start()

    # ---------- PUBLISH ----------
    def publish(self, record):
        try:
            self.q.put(record, timeout=0.01)
        except queue.Full:
            pass

    # ---------- STOP ----------
    def stop(self):
        self._running = False
        self._compress_event.set()

        if self._worker_thread:
            self._worker_thread.join()

        if self._compressor_thread:
            self._compressor_thread.join()

    # ---------- XLSX WORKER ----------
    def _xlsx_worker_loop(self):
        MAX_ROWS = 50_000

        start = time.time()
        sec_count = 0
        def open_new_workbook():
            workbook = Workbook(write_only=True)
            worksheet = workbook.create_sheet(title="log")
            count = 0

            if self.headers_list:
                worksheet.append(list(self.headers_list))
                count = 1

            return workbook, worksheet, count

        wb, ws, row_count = open_new_workbook()

        try:
            while self._running:
                end = time.time()
                try:
                    record = self.q.get(timeout=0.5)
                except queue.Empty:
                    continue

                ws.append(list(record))
                sec_count += 1
                row_count += 1

                if end - start >= 1.0:
                    print(f"[Worker] exc: {sec_count}")
                    sec_count = 0
                    start = end
                self.q.task_done()

                if row_count >= MAX_ROWS:
                    # finalize exceeded file
                    wb.save(self.file_manager.current_file)
                    wb.close()

                    # notify compressor AFTER close
                    self._compress_event.set()

                    # open new file immediately
                    self.file_manager.current_file = self.file_manager._new_log_file()
                    wb, ws, row_count = open_new_workbook()

        finally:
            wb.save(self.file_manager.current_file)
            wb.close()



    # ---------- COMPRESSOR ----------
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
