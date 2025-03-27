import json
import time

from config import DB_HOST, DB_USERNAME, DB_PASSWORD, DB_NAME, DB_PORT
import mysql.connector
from mysql.connector import Error

from logger import logger
from product import Product

# Khởi tạo kết nối toàn cục
connection = None
cursor = None

def init_db_connection():
    """Khởi tạo kết nối đến MySQL một lần duy nhất"""
    global connection, cursor
    try:
        connection = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USERNAME,
            password=DB_PASSWORD,
            database=DB_NAME,
            port=DB_PORT
        )
        if connection.is_connected():
            cursor = connection.cursor()
            logger.info("Đã kết nối đến MySQL thành công.")
    except Error as e:
        logger.info(f"Lỗi khi kết nối đến MySQL: {e}")

def close_db_connection():
    global connection, cursor
    if connection is not None and connection.is_connected():
        cursor.close()
        connection.close()
        logger.info("Đã đóng kết nối MySQL.")

def update_status_in_db(product: Product):
    global connection, cursor
    try:
        if connection is None or not connection.is_connected():
            logger.info("Kết nối đã đóng, đang khởi tạo lại...")
            init_db_connection()

        # Câu lệnh SQL để cập nhật status
        sql = "UPDATE tiki_craw_log SET status = %s, description = %s, duration = %s WHERE product_id = %s"

        # Chuyển dictionary thành JSON string
        description_str = json.dumps(product.description, ensure_ascii=False) if product.description else None
        data = (product.status, description_str, product.duration, product.product_id)

        # Thực thi câu lệnh
        start = time.time()
        cursor.execute(sql, data)
        connection.commit()

        end = time.time()
        logger.info(f"Save db in {end - start:.2f}s")

        if cursor.rowcount > 0:
            logger.info(
                f"Đã cập nhật status cho Product(product_id='{product.product_id}', status='{product.status}') trong cơ sở dữ liệu.")
        else:
            logger.info(f"Không tìm thấy product_id='{product.product_id}' để cập nhật.")

    except Error as e:
        logger.info(f"Lỗi khi cập nhật trong MySQL: {e}")


def select_all_product_ids(from_id: int, to_id: int):
    """Lấy tất cả product_id từ bảng products"""
    global connection, cursor
    try:
        if connection is None or not connection.is_connected():
            logger.info("Kết nối đã đóng, đang khởi tạo lại...")
            init_db_connection()

        sql = (f"select product_id from tiki_craw_log where (status is null or status != 200) "
               f"and (id between {from_id} and {to_id}) order by id ")
        cursor.execute(sql)

        # Lấy tất cả kết quả
        results = cursor.fetchall()

        # Trả về danh sách product_id (chỉ lấy cột đầu tiên từ mỗi dòng)
        product_ids = [row[0] for row in results]
        logger.info(f"Đã lấy được {len(product_ids)} product_id từ database.")
        return product_ids

    except Error as e:
        logger.info(f"Lỗi khi truy vấn MySQL: {e}")
        return []