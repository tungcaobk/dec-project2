import logging
import os
from datetime import datetime

# Tạo thư mục logs nếu chưa tồn tại
LOG_DIR = 'logs'
os.makedirs(LOG_DIR, exist_ok=True)

# Tạo tên file log với ngày hiện tại
log_filename = os.path.join(LOG_DIR, f"{datetime.now().strftime('%Y-%m-%d')}_application.log")

# Cấu hình logging
def setup_logging():
    # Cấu hình logger gốc
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[
            # Ghi log ra console
            logging.StreamHandler(),
            # Ghi log ra file
            logging.FileHandler(log_filename, encoding='utf-8')
        ]
    )

# Tạo logger toàn cục
logger = logging.getLogger("root")

# Gọi hàm setup logging khi import
setup_logging()