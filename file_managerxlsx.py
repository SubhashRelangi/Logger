import datetime
import gzip
import shutil
from pathlib import Path
from config import (
    MAX_FILE_SIZE_MB,
    LOG_DIRECTORY,
    LOG_DIRECTORY_MAX_SIZE_MB,
    MAX_COMPRESSION_PERCENT,
)


class FileManager:
    def __init__(
        self,
        file_type: str,
        log_directory: Path = LOG_DIRECTORY,
        max_file_size_mb: int = MAX_FILE_SIZE_MB,
        dir_max_size_mb: int = LOG_DIRECTORY_MAX_SIZE_MB,
        max_compress_percent: int = MAX_COMPRESSION_PERCENT,
        compress: bool = False,
    ):
        if not file_type:
            raise ValueError("file_type must be initialized")

        self.file_type = file_type.lstrip(".")
        self.log_dir = log_directory
        self.max_file_size = max_file_size_mb * 1024 * 1024
        self.dir_max_size = dir_max_size_mb * 1024 * 1024
        self.max_compress_percent = max_compress_percent
        self.compress = compress

        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.current_file = self._new_log_file()

    # ---------- FILE CREATION ----------
    def _new_log_file(self):
        ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S_%f")[:-3]
        path = self.log_dir / f"log_{ts}.{self.file_type}"

        if self.file_type != "xlsx":
            path.touch(exist_ok=False)

        return path

    # ---------- DIRECTORY SIZE ----------
    def directory_size(self):
        return sum(
            f.stat().st_size
            for f in self.log_dir.iterdir()
            if f.is_file() and not f.name.endswith(".gz")
        )

    # ---------- COMPRESSION ----------
    def compress_directory_if_needed(self):
        if not self.compress:
            return

        current_size = self.directory_size()
        if current_size < self.dir_max_size:
            return

        compression_target = self.dir_max_size * (self.max_compress_percent / 100)

        files = sorted(
            (
                f for f in self.log_dir.iterdir()
                if f.is_file()
                and not f.name.endswith(".gz")
                and f != self.current_file      # ❗ skip active file
            ),
            key=lambda f: f.stat().st_mtime,
        )

        for file in files:
            if self.directory_size() <= compression_target:
                break

            gz_path = file.with_name(file.name + ".gz")

            with open(file, "rb") as f_in, gzip.open(gz_path, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)

            file.unlink()
