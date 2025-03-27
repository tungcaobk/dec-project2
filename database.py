import json
import time
from config import DB_HOST, DB_USERNAME, DB_PASSWORD, DB_NAME, DB_PORT
from logger import logger
from product import Product
import mysql.connector
from mysql.connector import Error, pooling
import threading

# Sử dụng thread-local để lưu trữ kết nối cho mỗi thread
db_context = threading.local()

# Cấu hình kết nối
DB_CONFIG = {
    'host': DB_HOST,
    'user': DB_USERNAME,
    'password': DB_PASSWORD,
    'database': DB_NAME,
    'port': DB_PORT,
    'ssl_disabled': True,  # Tắt SSL nếu không cần thiết (có thể giúp tránh lỗi SSL)
    'autocommit': True,
    'pool_name': 'mypool',
    'pool_size': 20,
    'pool_reset_session': True,
    'connect_timeout': 10
}

# Khởi tạo connection pool
connection_pool = None


def init_db_pool():
    """Khởi tạo pool kết nối database"""
    global connection_pool
    try:
        connection_pool = pooling.MySQLConnectionPool(**DB_CONFIG)
        logger.info("Đã khởi tạo pool kết nối MySQL thành công.")
    except Error as e:
        logger.error(f"Lỗi khi khởi tạo pool kết nối MySQL: {e}")
        raise


def get_connection():
    """Lấy kết nối từ pool hoặc tạo kết nối mới nếu chưa có pool"""
    if not hasattr(db_context, 'connection') or db_context.connection is None:
        try:
            if connection_pool:
                db_context.connection = connection_pool.get_connection()
            else:
                # Fallback nếu không có pool
                db_context.connection = mysql.connector.connect(**{k: v for k, v in DB_CONFIG.items()
                                                                   if k not in ['pool_name', 'pool_size',
                                                                                'pool_reset_session']})
            logger.debug(f"Thread {threading.current_thread().name}: Đã tạo kết nối MySQL mới.")
        except Error as e:
            logger.error(f"Lỗi khi lấy kết nối MySQL: {e}")
            raise
    return db_context.connection


def get_cursor():
    """Lấy cursor từ kết nối hiện tại"""
    connection = get_connection()
    if not hasattr(db_context, 'cursor') or db_context.cursor is None:
        db_context.cursor = connection.cursor(dictionary=True)  # dictionary=True để trả về kết quả dạng dict
    return db_context.cursor


def close_connection():
    """Đóng kết nối hiện tại của thread"""
    if hasattr(db_context, 'cursor') and db_context.cursor:
        try:
            db_context.cursor.close()
        except Error as e:
            logger.warning(f"Lỗi khi đóng cursor: {e}")
        finally:
            db_context.cursor = None

    if hasattr(db_context, 'connection') and db_context.connection:
        try:
            if db_context.connection.is_connected():
                db_context.connection.close()
                logger.debug(f"Thread {threading.current_thread().name}: Đã đóng kết nối MySQL.")
        except Error as e:
            logger.warning(f"Lỗi khi đóng kết nối: {e}")
        finally:
            db_context.connection = None

def execute_query(query, params=None):
    """Thực thi truy vấn và tự động xử lý kết nối"""
    try:
        cursor = get_cursor()
        cursor.execute(query, params or ())
        return cursor
    except Error as e:
        logger.error(f"Lỗi khi thực thi truy vấn: {e}")
        # Thử kết nối lại nếu kết nối bị mất
        if "MySQL Connection not available" in str(e) or "Not connected" in str(e):
            # Xóa kết nối cũ
            db_context.connection = None
            db_context.cursor = None
            # Thử lại một lần nữa
            cursor = get_cursor()
            cursor.execute(query, params or ())
            return cursor
        raise

def fetch_all(query, params=None):
    """Thực thi truy vấn SELECT và trả về tất cả kết quả"""
    cursor = execute_query(query, params)
    return cursor.fetchall()

def fetch_one(query, params=None):
    """Thực thi truy vấn SELECT và trả về một kết quả"""
    cursor = execute_query(query, params)
    return cursor.fetchone()

def execute_update(query, params=None):
    """Thực thi truy vấn UPDATE, INSERT, DELETE"""
    cursor = execute_query(query, params)
    get_connection().commit()
    return cursor.rowcount

# Khởi tạo pool khi module được import
try:
    init_db_pool()
except Exception as e:
    logger.error(f"Không thể khởi tạo pool kết nối ban đầu: {e}")


def update_status_in_db(product: Product):
    """
    Cập nhật trạng thái của sản phẩm trong cơ sở dữ liệu sử dụng các hàm trợ giúp
    """
    try:
        # Câu lệnh SQL để cập nhật status
        sql = "UPDATE tiki_craw_log SET status = %s, description = %s, duration = %s WHERE product_id = %s"

        # Chuyển dictionary thành JSON string
        description_str = json.dumps(product.description, ensure_ascii=False) if product.description else None
        data = (product.status, description_str, product.duration, product.product_id)

        # Thực thi câu lệnh và ghi log thời gian
        start = time.time()
        rows_affected = execute_update(sql, data)
        end = time.time()

        logger.info(f"Save db in {end - start:.2f}s")

        if rows_affected > 0:
            logger.info(
                f"Đã cập nhật status cho Product(product_id='{product.product_id}', status='{product.status}') trong cơ sở dữ liệu.")
        else:
            logger.info(f"Không tìm thấy product_id='{product.product_id}' để cập nhật.")

    except Error as e:
        logger.error(f"Lỗi khi cập nhật trong MySQL: {e}")


def select_all_product_ids(from_id: int, to_id: int):
    """
    Lấy tất cả product_id từ bảng products sử dụng các hàm trợ giúp
    """
    try:
        sql = ("SELECT product_id FROM tiki_craw_log WHERE (status IS NULL) "
               "AND (id BETWEEN %s AND %s) ORDER BY id")

        # Sử dụng hàm fetch_all để lấy kết quả
        results = fetch_all(sql, (from_id, to_id))

        # Trả về danh sách product_id
        product_ids = [row['product_id'] for row in results]
        logger.info(f"Đã lấy được {len(product_ids)} product_id từ database.")
        return product_ids

    except Error as e:
        logger.error(f"Lỗi khi truy vấn MySQL: {e}")
        return []
