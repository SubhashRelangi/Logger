import shutil
from config import STORAGE_THRESHOLD_PERCENT

class SystemStorage:
    
    def __init__(self, threshold_value: int = STORAGE_THRESHOLD_PERCENT):
        
        self.threshold = threshold_value

    def checking(self):
        
        usage = shutil.disk_usage("/")
        used_percentage = (usage.used / usage.total) * 100

        if used_percentage >= self.threshold:
            print(f"can't initialize due to low memory (used={self.used_percent:.2f}%)")

        return True
