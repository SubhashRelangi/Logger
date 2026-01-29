import yaml
from pathlib import Path

KEY_MAP = {
    "STORAGE_THRESHOLD_PERCENT": ("storage", "threshold_percent"),
    "LOG_DIRECTORY_MAX_SIZE_MB": ("storage", "max_directory_size_mb"),
    "MAX_DIRECTORY_WARNING_THRESHOLD": ("storage", "max_dir_warning_threshold"),
    "MAX_FILES": ("storage", "max_files"),

    "LOG_DIRECTORY": ("logger", "directory"),
    "MAX_FILE_SIZE_MB": ("logger", "max_file_size_mb"),
    "QUEUE_SIZE": ("logger", "queue_size"),
    "DEFAULT_FILE_TYPE": ("logger", "default_file_type"),
    "DEFAULT_COMPRESS": ("logger", "default_compress"),
    "ENCODER": ("logger", "encoder"),

    "XLSX_MAX_ROWS": ("xlsxconfig", "rows"),
}

TYPE_MAP = {
    "STORAGE_THRESHOLD_PERCENT": int,
    "LOG_DIRECTORY_MAX_SIZE_MB": (int, float),
    "MAX_DIRECTORY_WARNING_THRESHOLD": int,
    "MAX_FILES": int,

    "LOG_DIRECTORY": str,
    "MAX_FILE_SIZE_MB": (int, float),
    "QUEUE_SIZE": int,
    "DEFAULT_FILE_TYPE": str,
    "DEFAULT_COMPRESS": bool,
    "ENCODER": bool,

    "XLSX_MAX_ROWS": int,
}


class LoggerConfig:
    def __init__(self, path="config.yaml"):
        self._path = path

        with open(path, "r") as f:
            data = yaml.safe_load(f)

        self.STORAGE_THRESHOLD_PERCENT = data["storage"]["threshold_percent"]
        self.LOG_DIRECTORY_MAX_SIZE_MB = data["storage"]["max_directory_size_mb"]
        self.MAX_DIRECTORY_WARNING_THRESHOLD = data["storage"]["max_dir_warning_threshold"]
        self.MAX_FILES = data["storage"]["max_files"]

        self.LOG_DIRECTORY = Path(data["logger"]["directory"])
        self.MAX_FILE_SIZE_MB = data["logger"]["max_file_size_mb"]
        self.QUEUE_SIZE = data["logger"]["queue_size"]
        self.DEFAULT_FILE_TYPE = data["logger"]["default_file_type"]
        self.DEFAULT_COMPRESS = data["logger"]["default_compress"]
        self.ENCODER = data["logger"]["encoder"]

        self.XLSX_MAX_ROWS = data["xlsxconfig"]["rows"]

    def update_config(self, updates: dict):
        with open(self._path, "r") as f:
            data = yaml.safe_load(f)

        for key, value in updates.items():
            if key not in KEY_MAP:
                raise KeyError(f"Unknown config key: {key}")

            expected = TYPE_MAP[key]
            if not isinstance(value, expected):
                raise TypeError(
                    f"{key} must be {expected}, got {type(value).__name__}"
                )

            section, field = KEY_MAP[key]

            if section not in data or field not in data[section]:
                raise KeyError(f"Invalid config path: {section}.{field}")

            data[section][field] = value

        tmp_path = Path(self._path).with_suffix(".tmp")
        with open(tmp_path, "w") as f:
            yaml.safe_dump(data, f, default_flow_style=False, sort_keys=False)

        tmp_path.replace(self._path)

        # Reload runtime values
        self.__init__(self._path)


settings = LoggerConfig()
