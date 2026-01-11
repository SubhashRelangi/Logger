import time
import gzip
import shutil
from pathlib import Path
from config import (
    MAX_FILE_SIZE_MB,
    LOG_DIRECTORY,
    LOG_DIRECTORY_MAX_SIZE_MB,
    MAX_COMPRESSION_PERCENT,
    LOG_TIMESTAMP_FORMAT,
)

class FileManager:

    def __init__(self, file_type: str, 
                log_directory: Path = LOG_DIRECTORY, 
                max_file_size_mb: int = MAX_FILE_SIZE_MB, 
                dir_max_size_mb: int = LOG_DIRECTORY_MAX_SIZE_MB, 
                max_compress_percent: int = MAX_COMPRESSION_PERCENT,
                compress: bool = False):
    
        if not file_type:
            print("file_type must be initialized")

        self.file_type = file_type.lstrip(".")
        self.log_dir = log_directory
        self.max_file_size = max_file_size_mb * 1024 * 1024
        self.dir_max_size = dir_max_size_mb * 1024 * 1024
        self.max_compress_percent = max_compress_percent
        self.compress = compress

        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.current_file = self._new_log_file()

    def _new_log_file(self):
        ts = time.strftime(LOG_TIMESTAMP_FORMAT)
        return self.log_dir / f"log_{ts}.{self.file_type}"

    def rotate_if_needed(self):
        if self.current_file.stat().st_size >= self.max_file_size:
            self.current_file = self._new_log_file()

    def directory_size(self):
        # total_size = 0

        # for f in self.log_dir.iterdir():
        #     if f.is_file():
        #         file_size = f.stat().st_size
        #         total_size += file_size

        # return total_size

        return sum(f.stat().st_size for f in self.log_dir.iterdir() if f.is_file())
    
    def compress_directory_if_needed(self):
        if self.compress:
            return
        
        if self.directory_size() < self.dir_max_size:
            return
        
        compression_size = self.dir_max_size * (self.max_compress_percent / 100)

        # files = []

        # for f in self.log_dir.iterdir():
        #     if not f.is_file():
        #         continue
        #     if f.name.endswith(".gz"):
        #         continue
        #     files.append(f)

        # # for f in files: # sorting the diles in the folder
        # #     f_key = f.stat().st_mtime # gets modification time of the file

        # # files.sort(by=f_key)


        # files.sort(key=lambda f: f.stat().st_mtime)

        files = sorted(
            (f for f in self.log_dir.iterdir() if f.is_file() and not f.name.endswith(".gz")),
            key=lambda f: f.stat().st_mtime,
        )

        for file in files:
            if self.directory_size() <= compression_size:
                break
            
            gz_path = file.with_name(file.name + ".gz")

            # f_in = open(file, "rb")
            # f_out = gzip.open(gz_path, "wb")

            # try:
            #     shutil.copyfileobj(f_in, f_out)
            # finally:
            #     f_in.close()
            #     f_out.close()

            with open(file, "rb") as f_in, gzip.open(gz_path, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out) # all files in the os level in the binary format

            file.unlink() # the converted file will be deleted
            

    
    
