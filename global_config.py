import yaml
from pathlib import Path

class LoggerConfig:
    def __init__(self, path="config.yaml"):
        with open(path, "r") as f:
            data = yaml.safe_load(f)
        
        # --- Storage Safety Settings ---
        self.STORAGE_THRESHOLD_PERCENT = data['storage']['threshold_percent']
        self.LOG_DIRECTORY_MAX_SIZE_MB = data['storage']['max_directory_size_mb']
        self.MAX_DIRECTORY_WARNING_THRESHOLD = data['storage']['max_dir_warning_threshold']
        self.MAX_FILES = data['storage']['max_files']

        # --- Logger Performance & Defaults ---
        self.LOG_DIRECTORY = Path(data['logger']['directory'])
        self.MAX_FILE_SIZE_MB = data['logger']['max_file_size_mb']
        self.QUEUE_SIZE = data['logger']['queue_size']
        self.DEFAULT_FILE_TYPE = data['logger']['default_file_type']
        self.DEFAULT_COMPRESS = data['logger']['default_compress']
        self.ENCODER = data['logger']['encoder']

        # --- XLSX Configs & Defaults ---
        self.XLSX_MAX_ROWS = data['xlsxconfig']['rows']

# This is the SHARED instance
settings = LoggerConfig()