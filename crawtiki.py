# import csv
# import re
# import json
# import requests
# import os
# import logging
# import time
# import threading
# import queue
# from concurrent.futures import ThreadPoolExecutor, as_completed
# import random
#
# def read_csv_to_list(file_path):
#     ids = []
#     with open(file_path, mode='r', newline='') as csvfile:
#         csvreader = csv.DictReader(csvfile)
#         for row in csvreader:
#             ids.append(row['id'])
#     return ids
#
# def get_product_info(product_id, sleep_time=0.5):
#     start = time.time()
#     url = f'https://api.tiki.vn/product-detail/api/v1/products/{product_id}'
#
#     user_agents = [
#         'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
#         'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36',
#         'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.0 Safari/605.1.15',
#         'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:90.0) Gecko/20100101 Firefox/90.0',
#         'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36',
#         'Mozilla/5.0 (iPhone; CPU iPhone OS 14_7_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.2 Mobile/15E148 Safari/604.1',
#         'Mozilla/5.0 (iPad; CPU OS 14_7_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.2 Mobile/15E148 Safari/604.1',
#         'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/92.0.902.73 Safari/537.36',
#     ]
#
#     # Chọn một User-Agent ngẫu nhiên
#     random_user_agent = random.choice(user_agents)
#
#     # Tạo headers với User-Agent ngẫu nhiên
#     headers = {
#         'User-Agent': random_user_agent,
#         'Accept': 'application/json, text/plain, */*',
#         'Accept-Language': 'en-US,en;q=0.9,vi;q=0.8',
#         'Referer': 'https://tiki.vn/',
#         'Connection': 'keep-alive'
#     }
#
#     # Thêm một số headers ngẫu nhiên khác
#     if random.random() > 0.5:
#         headers['Accept-Encoding'] = 'gzip, deflate, br'
#     if random.random() > 0.7:
#         headers['Cache-Control'] = 'no-cache'
#     # headers = {
#     #     'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
#     # }
#     response = requests.get(url, headers=headers)
#
#     time.sleep(sleep_time)  # Delay để tránh bị rate limit
#
#     end = time.time()
#     logging.info(f"ID {product_id} - Time: {end - start:.2f}s - Status code: {response.status_code}")
#     if response.status_code == 200:
#         data = response.json()
#
#         # Trích xuất các thông tin cần thiết
#         extracted_info = {
#             'id': data.get('id'),
#             'name': data.get('name'),
#             'url_key': data.get('url_key'),
#             'price': data.get('price'),
#             'description': normalize_description(data.get('description', '')),
#             'images_url': [img.get('base_url') for img in data.get('images', [])]
#         }
#
#         return extracted_info
#     elif response.status_code == 429:
#         # Kiểm tra header để xem cần đợi bao lâu
#         if 'Retry-After' in response.headers:
#             retry_after = int(response.headers['Retry-After'])
#             logging.info(f"Rate limited. Waiting {retry_after} seconds as specified by API")
#             # time.sleep(retry_after)
#             # return get_with_rate_limit_handling(url)
#     else:
#         logging.info(f"Error: {response.status_code} for product ID {product_id}")
#         return None
#
# def normalize_description(description):
#     if not description:
#         return ""
#
#     # Loại bỏ các thẻ HTML
#     description = re.sub(r'</?[^>]+>', '', description)
#
#     # Thay thế nhiều khoảng trắng liên tiếp bằng một khoảng trắng
#     description = re.sub(r'\s+', ' ', description)
#
#     # Chuẩn hóa danh sách bằng dấu gạch đầu dòng
#     description = re.sub(r'(?<=\n)•\s*', '- ', description)
#     description = re.sub(r'(?<=\n)\*\s*', '- ', description)
#
#     # Thay thế các ký tự đặc biệt
#     description = description.replace('&amp;', '&')
#     description = description.replace('&lt;', '<')
#     description = description.replace('&gt;', '>')
#     description = description.replace('&quot;', '"')
#
#     # Loại bỏ phần disclaimer về giá ở cuối mô tả
#     description = re.sub(r'Giá sản phẩm trên Tiki.*?\.\.\.\.\.', '', description)
#
#     # Loại bỏ các khoảng trắng thừa ở đầu và cuối
#     description = description.strip()
#
#     return description
#
#
# def process_with_thread_pool(id_list, output_folder="product_data", max_workers=10):
#     """
#     Xử lý đa luồng danh sách ID sản phẩm sử dụng ThreadPoolExecutor
#
#     Args:
#         id_list (list): Danh sách ID sản phẩm cần xử lý
#         output_folder (str): Thư mục để lưu các file JSON
#         max_workers (int): Số lượng thread tối đa
#     """
#     # Tạo thư mục nếu chưa tồn tại
#     if not os.path.exists(output_folder):
#         os.makedirs(output_folder)
#
#     # Danh sách kết quả và ID thất bại
#     results = []
#     failed_ids = []
#
#     # Khóa để đồng bộ hóa việc ghi kết quả
#     results_lock = threading.Lock()
#
#     batch_size = 1000
#
#     def process_id(product_id):
#         try:
#             product_info = get_product_info(product_id)
#             if product_info:
#                 with results_lock:
#                     results.append(product_info)
#
#                     # Kiểm tra nếu đạt đến batch_size
#                     if len(results) % batch_size == 0:
#                         batch_number = len(results) // batch_size
#                         save_batch(results[-batch_size:], output_folder, batch_number)
#
#                 return True
#             else:
#                 with results_lock:
#                     failed_ids.append(product_id)
#                 return False
#         except Exception as e:
#             logging.error(f"Lỗi khi xử lý sản phẩm ID {product_id}: {str(e)}")
#             with results_lock:
#                 failed_ids.append(product_id)
#             return False
#
#     logging.info(f"Bắt đầu xử lý {len(id_list)} sản phẩm với {max_workers} luồng...")
#
#     # Sử dụng ThreadPoolExecutor để quản lý thread
#     with ThreadPoolExecutor(max_workers=max_workers) as executor:
#         # Thực thi các tác vụ
#         futures = {executor.submit(process_id, product_id): product_id for product_id in id_list}
#
#         # Theo dõi tiến trình
#         completed = 0
#         for future in futures:
#             product_id = futures[future]
#             try:
#                 success = future.result()
#                 completed += 1
#                 if completed % 100 == 0:
#                     logging.info(f"Đã hoàn thành {completed}/{len(id_list)} sản phẩm")
#             except Exception as e:
#                 logging.error(f"Lỗi khi xử lý sản phẩm ID {product_id}: {str(e)}")
#                 with results_lock:
#                     failed_ids.append(product_id)
#
#     # Lưu phần còn lại nếu có
#     remaining = len(results) % batch_size
#     if remaining > 0:
#         batch_number = (len(results) // batch_size) + 1
#         save_batch(results[-remaining:], output_folder, batch_number)
#
#     # Lưu danh sách ID thất bại (nếu có)
#     if failed_ids:
#         with open(f"{output_folder}/failed_ids.json", 'w', encoding='utf-8') as f:
#             json.dump(failed_ids, f)
#         logging.info(f"Đã lưu {len(failed_ids)} ID thất bại vào file {output_folder}/failed_ids.json")
#
#     logging.info(f"Hoàn thành! Đã xử lý thành công {len(results)}/{len(id_list)} sản phẩm")
#     return len(results)
#
# def save_batch(products_data, output_folder, batch_number):
#     """Lưu một batch sản phẩm vào file"""
#     filename = f"{output_folder}/products_batch_{batch_number}.json"
#     with open(filename, 'w', encoding='utf-8') as f:
#         json.dump(products_data, f, ensure_ascii=False, indent=2)
#     logging.info(f"Đã lưu {len(products_data)} sản phẩm vào file {filename}")
#
#
# def fetch_products_sequentially(id_list):
#     """
#     Lấy thông tin sản phẩm tuần tự để tránh lỗi 429
#
#     Args:
#         id_list (list): Danh sách ID sản phẩm cần xử lý
#     Returns:
#         list: Danh sách thông tin sản phẩm đã lấy được
#     """
#     results = []
#     failed_ids = []
#
#     for index, product_id in enumerate(id_list, 1):
#         try:
#             product_info = get_product_info(product_id, 0.1)
#             if product_info:
#                 results.append(product_info)
#             else:
#                 failed_ids.append(product_id)
#
#             # In tiến độ
#             if index % 100 == 0:
#                 logging.info(f"Đã fetch {index}/{len(id_list)} sản phẩm")
#
#             # Delay between requests để tránh rate limit
#             # time.sleep(0.5)  # Điều chỉnh thời gian delay tùy theo rate limit
#
#         except Exception as e:
#             logging.error(f"Lỗi khi fetch sản phẩm ID {product_id}: {str(e)}")
#             failed_ids.append(product_id)
#
#     # Lưu danh sách ID thất bại
#     if failed_ids:
#         with open("failed_ids.json", 'w', encoding='utf-8') as f:
#             json.dump(failed_ids, f)
#         logging.info(f"Đã lưu {len(failed_ids)} ID thất bại vào file failed_ids.json")
#
#     return results
#
# def save_products_parallel(products_data, output_folder="product_data", max_workers=10):
#     """
#     Lưu danh sách sản phẩm vào files JSON sử dụng đa luồng
#
#     Args:
#         products_data (list): Danh sách thông tin sản phẩm
#         output_folder (str): Thư mục để lưu các file JSON
#         max_workers (int): Số lượng thread tối đa
#     """
#     if not os.path.exists(output_folder):
#         os.makedirs(output_folder)
#
#     batch_size = 1000
#     total_batches = (len(products_data) + batch_size - 1) // batch_size
#
#     def save_batch(batch_number):
#         start_idx = batch_number * batch_size
#         end_idx = min(start_idx + batch_size, len(products_data))
#         batch_data = products_data[start_idx:end_idx]
#
#         filename = f"{output_folder}/products_batch_{batch_number + 1}.json"
#         with open(filename, 'w', encoding='utf-8') as f:
#             json.dump(batch_data, f, ensure_ascii=False, indent=2)
#         logging.info(f"Đã lưu batch {batch_number + 1}/{total_batches} ({len(batch_data)} sản phẩm)")
#
#     logging.info(f"Bắt đầu lưu {len(products_data)} sản phẩm với {max_workers} luồng...")
#
#     with ThreadPoolExecutor(max_workers=max_workers) as executor:
#         # Submit các tác vụ lưu file
#         futures = [executor.submit(save_batch, i) for i in range(total_batches)]
#
#         # Đợi tất cả các tác vụ hoàn thành
#         for future in as_completed(futures):
#             try:
#                 future.result()
#             except Exception as e:
#                 logging.info(f"Lỗi khi lưu batch: {str(e)}")
#
#     logging.info(f"Hoàn thành! Đã lưu {len(products_data)} sản phẩm thành {total_batches} files")
#
# def main():
#     os.makedirs('logs', exist_ok=True)
#     # Cấu hình logging với đường dẫn đến thư mục logs
#     logging.basicConfig(
#         filename='logs/crawtiki.log',
#         level=logging.INFO,
#         format='%(asctime)s - %(message)s'
#     )
#     # file_path = '/Users/macbook/TungCao/PythonProject/tikicraw/products_id_input.csv'
#     file_path = '/home/ubuntu/learning/tikicraw/products-0-200000.csv'
#
#     id_list = read_csv_to_list(file_path)
#
#     # Bước 1: Fetch dữ liệu tuần tự
#     logging.info("Bắt đầu fetch dữ liệu sản phẩm...")
#     products_data = fetch_products_sequentially(id_list)
#     logging.info(f"Đã fetch xong {len(products_data)} sản phẩm")
#
#     # Bước 2: Lưu dữ liệu đa luồng
#     logging.info("Bắt đầu lưu dữ liệu...")
#     save_products_parallel(products_data)
#
# if __name__ == "__main__":
#     main()
# # if __name__ == "__main__":
# #     file_path = '/Users/macbook/TungCao/PythonProject/tikicraw/products_id_input.csv'
# #     id_list = read_csv_to_list(file_path)
# #     process_with_thread_pool(id_list, "tiki_products_output", max_workers=2)
#
# # logging.info(f"Total IDs extracted: {len(id_list)}")
# # logging.info(f"First 5 IDs: {id_list[:5]}")
#
# # Sử dụng hàm
# # product_info = get_product_info(id_list[0])
#
# # if product_info:
# #     logging.info("First product info")
# #     logging.info(json.dumps(product_info, ensure_ascii=False, indent=2))
# # else:
# #     logging.info("Không thể lấy thông tin sản phẩm")
