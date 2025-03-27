import time
from config import TASK_FROM, TASK_TO, NUM_THREAD
from database import select_all_product_ids, update_status_in_db, close_connection
from logger import logger
from third_party import get_product_info
import concurrent.futures

def process_product(product_id):
    try:
        response = get_product_info(product_id)
        update_status_in_db(response)
    except Exception as e:
        logger.error(f"Lỗi khi xử lý product_id {product_id}: {e}")
    finally:
        # Đóng kết nối khi hoàn thành
        close_connection()


def run_fetch_data():
    start = time.time()

    product_ids = select_all_product_ids(TASK_FROM, TASK_TO)
    if not product_ids:
        logger.info("Không có product_id nào để xử lý.")
        return

    with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_THREAD) as executor:
        executor.map(process_product, product_ids)

    end = time.time()
    logger.info(f"Finished task in [{end - start:.2f}s]")
    close_connection()

if __name__ == "__main__":
    run_fetch_data()