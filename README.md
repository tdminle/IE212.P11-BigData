# Phân loại cảm xúc đánh giá sản phẩm Shoppe

Đồ án phân tích sắc thái ngôn từ sử dụng các mô hình học máy và dữ liệu từ Shopee API. Đồ án này sử dụng các công nghệ như Kafka, Pyspark, và mô hình học máy để xử lý và phân loại cảm xúc các bình luận từ Shopee.

## Cài đặt

Để cài đặt tất cả các gói phụ thuộc cho ứng dụng, bạn cần cài đặt các gói từ file `requirements.txt`. 

### Cài đặt các gói cần thiết:
1. Clone repository của dự án:
    ```bash
    git clone https://github.com/tdminle/IE212.P11-BigData.git
    ```

2. Cài đặt các gói yêu cầu:
    ```bash
    pip install -r requirements.txt
    ```

## Sử dụng

**Kafka Broker**: Đảm bảo rằng Kafka broker đang chạy trên máy của bạn. Bạn có thể chỉ định địa chỉ broker trong file cấu hình như sau:
    ```python
    KAFKA_BROKER = "localhost:9092"
    ```
    
### Chạy ứng dụng

Sau khi cài đặt xong các gói phụ thuộc, bạn có thể chạy ứng dụng bằng cách thực hiện các bước sau:

1. **Train mô hình** :
    
    ```bash
    IE212_project.ipynb
    ```

2. **Xử lý dữ liệu và phân loại cảm xúc**:
    ```bash
    python consumer.py
    python streaming.py
    python producer.py
    ```

### Cấu hình Kafka




**Kafka Topics**: Các topic trong Kafka:
    - `rawData`: Nơi lưu trữ các bình luận thô.
    - `resultData`: Nơi lưu trữ kết quả phân loại cảm xúc.

### Các tham số cấu hình khác

**Shopee API**: Đảm bảo rằng bạn cung cấp đúng `item_id` và `shop_id` trong các script thu thập dữ liệu từ Shopee API.

