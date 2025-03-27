import time

from config import TASK_FROM, TASK_TO, NUM_THREAD
from database import select_all_product_ids, init_db_connection, close_db_connection, update_status_in_db
from logger import logger
from third_party import get_product_info
import concurrent.futures
from threading import Lock

# Thêm lock để tránh xung đật khi ghi vào database
db_lock = Lock()

def process_product(product_id):
    try:
        response = get_product_info(product_id)
        with db_lock:  # Đảm bảo thread-safe khi ghi vào DB
            update_status_in_db(response)
    except Exception as e:
        logger.error(f"Lỗi khi xử lý product_id {product_id}: {e}")

def run_fetch_data():
    start = time.time()

    init_db_connection()
    product_ids = select_all_product_ids(TASK_FROM, TASK_TO)
    if not product_ids:
        logger.info("Không có product_id nào để xử lý.")
        close_db_connection()
        return

    with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_THREAD) as executor:
        executor.map(process_product, product_ids)

    end = time.time()
    logger.info(f"Finished task in [{end - start:.2f}s]")
    close_db_connection()

    # for product_id in product_ids[0:3]:
    #     response = get_product_info(product_id)
    #     update_status_in_db(response)

if __name__ == "__main__":
    run_fetch_data()