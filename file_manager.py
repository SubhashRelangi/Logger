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
                max_dir_size_warning: int = None,
                compress: bool = False):
        
        self.file_type = file_type.lstrip(".")
        self.log_dir = log_directory or settings.LOG_DIRECTORY
        self.max_file_size = (max_file_size_mb or settings.MAX_FILE_SIZE_MB) * 1024 * 1024
        self.dir_max_size = (dir_max_size_mb or settings.LOG_DIRECTORY_MAX_SIZE_MB) * 1024 * 1024
        self.max_dir_size_warning = (max_dir_size_warning or settings.MAX_DIRECTORY_WARNING_THRESHOLD) * 1024 * 1024
        self.compress = compress


        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.current_file = self._new_log_file()

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
        # Compress oldest non-gz file
        sorted_files = sorted(
            (
                f for f in self.log_dir.iterdir()
                if f.is_file()
                and not f.name.endswith(".gz")
                and f != self.current_file
            ),
            key=lambda f: f.stat().st_mtime,
        )

        if not sorted_files:
            return

        compress_file = sorted_files[0]
        self.compress_worker(compress_file)

        dir_size = self.directory_size()

        if dir_size >= self.max_dir_size_warning:
            print(
                f"[WARNING] {self.log_dir} exceeds "
                f"{self.max_dir_size_warning // (1024 * 1024)} MB!"
            )

        if dir_size >= self.dir_max_size:
            gz_files = self.gz_files_sort()

            for old_gz in gz_files:
                size = old_gz.stat().st_size
                old_gz.unlink()
                print(f"[Logger] Deleted {old_gz} to free space.")
                dir_size -= size
                if dir_size < self.dir_max_size:
                    break

        if dir_size >= self.dir_max_size:
            raise Exception(
                f"[CRITICAL] Logging stopped: {self.log_dir} exceeds "
                f"{self.dir_max_size // (1024 * 1024)} MB."
            )
    
    # def compress_directory_if_needed(self):
    #     if not self.compress:
    #         return
    #     current_size = self.directory_size()
    #     if current_size < self.dir_max_size:
    #         return
        
    #     compression_size = self.dir_max_size * (self.max_compress_percent / 100)

    #     # files = []

    #     # for f in self.log_dir.iterdir():
    #     #     if not f.is_file():
    #     #         continue
    #     #     if f.name.endswith(".gz"):
    #     #         continue
    #     #     files.append(f)

    #     # # for f in files: # sorting the diles in the folder
    #     # #     f_key = f.stat().st_mtime # gets modification time of the file

    #     # # files.sort(by=f_key)


    #     # files.sort(key=lambda f: f.stat().st_mtime)

    #     # Why we are sorting the files because by this sorting comes first written file like older -> newer

    #     files = sorted(
    #         (f for f in self.log_dir.iterdir() if f.is_file() and not f.name.endswith(".gz")),
    #         key=lambda f: f.stat().st_mtime,
    #     )

    #     for file in files:
    #         if self.directory_size() <= compression_size:
    #             break
            
    #         gz_path = file.with_name(file.name + ".gz")

    #         # f_in = open(file, "rb")
    #         # f_out = gzip.open(gz_path, "wb")

    #         # try:
    #         #     shutil.copyfileobj(f_in, f_out)
    #         # finally:
    #         #     f_in.close()
    #         #     f_out.close()

    #         # Lossless compression formats reduce disk usage while preserving data exactly; loggers must use streaming-friendly lossless formats like gzip or zstd.
    #         with open(file, "rb") as f_in, gzip.open(gz_path, "wb") as f_out:
    #             shutil.copyfileobj(f_in, f_out) # all files in the os level in the binary format

    #         size = file.stat().st_size
    #         file.unlink() # the converted file will be deleted
    #         current_size -= size