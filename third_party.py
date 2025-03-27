import time
import random
import requests
import re
from config import API_URL, DB_HOST, DB_USERNAME, DB_PASSWORD, DB_NAME, DB_PORT
from logger import logger
from product import Product

# Hàm lấy thông tin sản phẩm với xử lý exception
def get_product_info(product_id, sleep_time=0.1):
    logger.info(f"Current product id: {product_id}")
    start = time.time()
    url = API_URL + product_id
    headers = create_headers()
    product = Product.from_product_id(product_id)
    response = None  # Khởi tạo response

    try:
        response = requests.get(url, headers=headers)
        product.status = response.status_code
        response.raise_for_status()

        if response.status_code == 200:
            data = response.json()
            extracted_info = {
                'id': data.get('id'),
                'name': data.get('name'),
                'url_key': data.get('url_key'),
                'price': data.get('price'),
                'description': normalize_description(data.get('description', '')),
                'images_url': [img.get('base_url') for img in data.get('images', [])]
            }
            product.description = extracted_info
            # logger.info(f"{product}")

    except requests.exceptions.HTTPError as e:
        logger.error(f"HTTP Error: {e}")
        product.error = f"HTTP Error: {e}"
    except requests.exceptions.RequestException as e:
        logger.error(f"Request Exception: {e}")
        product.error = f"Request Exception: {e}"

    end = time.time()
    product.duration = (end - start) * 1000

    time.sleep(sleep_time)

    # Kiểm tra response tồn tại trước khi log status_code
    status_code = response.status_code if response else "N/A"
    logger.info(f"ID {product_id} - Time: {end - start:.2f}s - Status code: {status_code} "
                f"- duration: {product.duration:.2f}ms")

    return product

# Hàm chọn User-Agent ngẫu nhiên
def get_random_user_agent():
    user_agents = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.0 Safari/605.1.15',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:90.0) Gecko/20100101 Firefox/90.0',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36',
        'Mozilla/5.0 (iPhone; CPU iPhone OS 14_7_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.2 Mobile/15E148 Safari/604.1',
        'Mozilla/5.0 (iPad; CPU OS 14_7_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.2 Mobile/15E148 Safari/604.1',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/92.0.902.73 Safari/537.36',
    ]
    return random.choice(user_agents)

# Hàm tạo headers với các giá trị ngẫu nhiên
def create_headers():
    headers = {
        'User-Agent': get_random_user_agent(),
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-US,en;q=0.9,vi;q=0.8',
        'Referer': 'https://tiki.vn/',
        'Connection': 'keep-alive'
    }
    if random.random() > 0.5:
        headers['Accept-Encoding'] = 'gzip, deflate, br'
    if random.random() > 0.7:
        headers['Cache-Control'] = 'no-cache'
    return headers

# Hàm giả định để chuẩn hóa mô tả sản phẩm
def normalize_description(description):
    if not description:
        return ""

    # Loại bỏ các thẻ HTML
    description = re.sub(r'</?[^>]+>', '', description)

    # Thay thế nhiều khoảng trắng liên tiếp bằng một khoảng trắng
    description = re.sub(r'\s+', ' ', description)

    # Chuẩn hóa danh sách bằng dấu gạch đầu dòng
    description = re.sub(r'(?<=\n)•\s*', '- ', description)
    description = re.sub(r'(?<=\n)\*\s*', '- ', description)

    # Thay thế các ký tự đặc biệt
    description = description.replace('&amp;', '&')
    description = description.replace('&lt;', '<')
    description = description.replace('&gt;', '>')
    description = description.replace('&quot;', '"')

    # Loại bỏ phần disclaimer về giá ở cuối mô tả
    description = re.sub(r'Giá sản phẩm trên Tiki.*?\.\.\.\.\.', '', description)

    # Loại bỏ các khoảng trắng thừa ở đầu và cuối
    description = description.strip()

    return description