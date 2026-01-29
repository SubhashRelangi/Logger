import time
import datetime
import gzip
import shutil
from pathlib import Path
from global_config import settings

class FileManager:

    def __init__(self, file_type: str, 
                log_directory: Path = None, # Use None here
                max_file_size_mb: int = None, # Use None here
                dir_max_size_mb: int = None, 
                compress: bool = False):
        
        self.file_type = file_type.lstrip(".")
        self.log_dir = log_directory or settings.LOG_DIRECTORY
        self.max_file_size = (max_file_size_mb or settings.MAX_FILE_SIZE_MB) * 1024 * 1024
        self.dir_max_size = (dir_max_size_mb or settings.LOG_DIRECTORY_MAX_SIZE_MB) * 1024 * 1024
        self.compress = compress
        self.max_uncompressed_files = settings.MAX_FILES

        self.warning_bytes = (
            settings.LOG_DIRECTORY_MAX_SIZE_MB *
            settings.MAX_DIRECTORY_WARNING_THRESHOLD / 100 *
            1024 * 1024
        )


        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.current_file = self._new_log_file()
        self.gz_files_to_delete = 0
        self.gz_deleted_size = 0

    def _new_log_file(self):
        ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S_%f")[:-3]
        path = self.log_dir / f"log_{ts}.{self.file_type}"
        self.log_dir.mkdir(parents=True, exist_ok=True)
        path.touch(exist_ok=False)
        return path



    def directory_size(self):
        # total_size = 0

        # for f in self.log_dir.iterdir():
        #     if f.is_file():
        #         file_size = f.stat().st_size
        #         total_size += file_size

        # return total_size

        return sum(f.stat().st_size for f in self.log_dir.iterdir() if f.is_file())
    
    #=================================== COMPRESSOR ======================================
    def compress_worker(self, compress_file):
        
        gz_path = compress_file.with_suffix(compress_file.suffix + ".gz")

        with open(compress_file, "rb") as f_in, gzip.open(gz_path, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)
            compress_file.unlink()

    #=================================== GZ_SORTER ========================================
    def gz_files_sort(self):
        files = sorted(
            (f for f in self.log_dir.rglob("*.gz") if f.is_file()),
            key=lambda f: f.stat().st_mtime
        )

        return files
    

    #================================= COMPRESSOR LOGS ====================================
    def compress_logs(self):

        if not self.compress:
            return


        csv_files = [
            f for f in self.log_dir.iterdir()
            if f.is_file() and f.suffix == f".{self.file_type}"
        ]

        csv_files.sort(key=lambda f: f.stat().st_mtime, reverse=True)

        for old_file in csv_files[self.max_uncompressed_files:]:
            self.compress_worker(old_file)

        dir_size = self.directory_size()


        if dir_size >= self.warning_bytes:
            print(
                f"[WARNING] {self.log_dir} exceeds "
                f"{self.warning_bytes / 1000000} MB!"
            )

        if dir_size >= self.dir_max_size:

            gz_files = sorted(
                (
                    f for f in self.log_dir.iterdir()
                    if f.is_file() and f.suffix == ".gz"
                ),
                key=lambda f: f.stat().st_mtime  
            )

            self.gz_files_to_delete = self.max_file_size - self.current_file.stat().st_size
        
            for old_gz in gz_files:
                size = old_gz.stat().st_size
                self.gz_deleted_size += size
                old_gz.unlink()
                print(f"[Logger] Deleted {old_gz} to free space.")
                dir_size -= size
                # print(f"gz_files_to_delete: {self.gz_files_to_delete} and current_file: {self.current_file.stat().st_size} and gz_deleted_size: {self.gz_deleted_size}")
                
                if self.gz_deleted_size >= self.gz_files_to_delete:
                    raise RuntimeError(
                    f"[CRITICAL] Logging stopped: {self.log_dir} exceeds "
                    f"{self.dir_max_size // (1024 * 1024)} MB."
                )

                if dir_size < self.dir_max_size:
                    break

    