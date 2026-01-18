import requests
import json
import time
from kafka import KafkaProducer
from kafka.errors import KafkaError
from datetime import datetime
import logging

# Cấu hình logging để theo dõi hoạt động
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Kafka Producer configuration
KAFKA_BROKER = "localhost:9092"
TOPIC_RAW_DATA = "rawData"

# Khởi tạo Kafka producer với cấu hình tối ưu
# CẢNH BÁO: Producer được khởi tạo global, cần đóng đúng cách khi kết thúc
producer = None

def init_producer():
    """
    Khởi tạo Kafka producer với error handling
    Returns:
        KafkaProducer: Kafka producer instance hoặc None nếu lỗi
    """
    global producer
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
            acks='all',  # Đảm bảo message được ghi vào tất cả replicas
            retries=3,   # Retry 3 lần nếu gửi thất bại
            max_in_flight_requests_per_connection=1  # Đảm bảo thứ tự message
        )
        logger.info("Kafka producer đã được khởi tạo thành công")
        return producer
    except Exception as e:
        logger.error(f"Lỗi khi khởi tạo Kafka producer: {e}")
        return None

def fetch_shopee_comments(item_id, shop_id, limit=10, offset=0):
    """
    Lấy dữ liệu bình luận từ API Shopee
    
    Args:
        item_id (int): ID sản phẩm trên Shopee
        shop_id (int): ID cửa hàng trên Shopee
        limit (int): Số lượng bình luận cần lấy (mặc định: 10)
        offset (int): Vị trí bắt đầu lấy dữ liệu (mặc định: 0)
    
    Returns:
        list: Danh sách bình luận hoặc list rỗng nếu lỗi
    """
    url = "https://shopee.vn/api/v4/item/get_ratings"
    params = {
        "itemid": item_id,
        "shopid": shop_id,
        "limit": limit,
        "offset": offset,  # Dùng offset để lấy tiếp bình luận mới
        "filter": 0,  # 0: tất cả, 1: có hình ảnh, 2: 5 sao, 3: 4 sao, ...
        "type": 0     # 0: bình thường
    }

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Accept": "application/json"
    }

    try:
        # Thêm timeout để tránh treo vô thời hạn
        response = requests.get(url, headers=headers, params=params, timeout=10)
        response.raise_for_status()  # Raise exception cho status code lỗi
        
        data = response.json()
        comments = data.get("data", {}).get("ratings", [])
        logger.info(f"Đã lấy {len(comments)} bình luận từ offset {offset}")
        return comments
        
    except requests.exceptions.Timeout:
        logger.error(f"Timeout khi gọi API Shopee (offset={offset})")
        return []
    except requests.exceptions.RequestException as e:
        logger.error(f"Lỗi kết nối API: {e}")
        return []
    except json.JSONDecodeError as e:
        logger.error(f"Lỗi parse JSON response: {e}")
        return []
    except Exception as e:
        logger.error(f"Lỗi không xác định: {e}")
        return []

def send_to_kafka(data):
    """
    Gửi dữ liệu vào Kafka topic với error handling
    
    Args:
        data (dict): Dữ liệu cần gửi
    
    Returns:
        bool: True nếu gửi thành công, False nếu thất bại
    """
    if producer is None:
        logger.error("Producer chưa được khởi tạo")
        return False
    
    try:
        # Gửi message và nhận future object
        future = producer.send(TOPIC_RAW_DATA, data)
        # Đợi confirm từ Kafka (timeout 10s)
        record_metadata = future.get(timeout=10)
        
        logger.debug(f"Message đã gửi đến topic {record_metadata.topic}, "
                    f"partition {record_metadata.partition}, "
                    f"offset {record_metadata.offset}")
        return True
        
    except KafkaError as e:
        logger.error(f"Lỗi Kafka khi gửi message: {e}")
        return False
    except Exception as e:
        logger.error(f"Lỗi không xác định khi gửi message: {e}")
        return False


def get_and_send_comments(item_id, shop_id, offset=0):
    """
    Lấy và gửi bình luận vào Kafka
    
    Args:
        item_id (int): ID sản phẩm
        shop_id (int): ID cửa hàng
        offset (int): Vị trí bắt đầu
    
    Returns:
        tuple: (có_dữ_liệu_mới, offset_mới)
    """
    limit = 10
    new_data_received = False
    successful_sends = 0

    comments = fetch_shopee_comments(item_id, shop_id, limit, offset)

    if comments:
        for comment in comments:
            # Validate và xử lý dữ liệu an toàn
            try:
                # Lấy timestamp với giá trị mặc định
                timestamp = comment.get("ctime", 0)
                if timestamp > 0:
                    time_str = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
                else:
                    time_str = "N/A"
                
                data = {
                    "rating": str(comment.get("rating_star", "N/A")),  # Đảm bảo kiểu string
                    "comment": str(comment.get("comment", "N/A")),      # Đảm bảo kiểu string
                    "orderid": str(comment.get("orderid", "N/A")),      # FIX: Sửa key từ "orderID" thành "orderid" để khớp với schema
                    "time": time_str
                }
                
                # Gửi bình luận vào Kafka
                if send_to_kafka(data):
                    successful_sends += 1
                    new_data_received = True
                else:
                    logger.warning(f"Không gửi được comment offset={offset}")
                    
            except Exception as e:
                logger.error(f"Lỗi khi xử lý comment: {e}")
                continue

        # Cập nhật offset cho lần lấy tiếp theo
        offset += limit
        logger.info(f"Đã gửi thành công {successful_sends}/{len(comments)} comments. Offset mới: {offset}")
    else:
        logger.info("Không có comment mới.")

    return new_data_received, offset

def main():
    """
    Hàm chính: Thu thập dữ liệu định kỳ từ Shopee và gửi vào Kafka
    """
    # Cấu hình sản phẩm cần crawl
    item_id = 22088583698  # ID sản phẩm (thay bằng ID sản phẩm của bạn)
    shop_id = 196261835    # ID shop (thay bằng ID shop của bạn)
    
    offset = 0  # Biến lưu trữ offset của bình luận cuối cùng đã lấy
    max_iterations = 1000  # Giới hạn số lần lặp để tránh chạy vô hạn
    sleep_interval = 5  # Thời gian chờ giữa các request (giây)

    # Khởi tạo Kafka producer
    if init_producer() is None:
        logger.error("Không thể khởi tạo Kafka producer. Thoát chương trình.")
        return

    try:
        for iteration in range(max_iterations):
            try:
                logger.info(f"Iteration {iteration + 1}/{max_iterations}: Gửi request lấy dữ liệu bình luận...")
                new_data_received, offset = get_and_send_comments(item_id, shop_id, offset)
                
                if not new_data_received:
                    logger.info(f"Không có comment mới, sẽ thử lại sau {sleep_interval}s...")
                
                # Chờ trước khi gửi request tiếp theo (tránh spam API)
                time.sleep(sleep_interval)
                
            except Exception as e:
                logger.error(f"Lỗi trong iteration {iteration + 1}: {e}")
                time.sleep(sleep_interval)  # Vẫn sleep để tránh loop quá nhanh
                
    except KeyboardInterrupt:
        logger.info("Nhận tín hiệu ngắt (Ctrl+C). Đang dừng chương trình...")
    finally:
        # Cleanup: Đóng producer đúng cách
        if producer is not None:
            try:
                producer.flush()  # Đảm bảo tất cả message đã được gửi
                producer.close()  # Đóng connection
                logger.info("Đã đóng Kafka producer")
            except Exception as e:
                logger.error(f"Lỗi khi đóng producer: {e}")
        
        logger.info("Kết thúc quá trình crawl dữ liệu.")

if __name__ == "__main__":
    main()

