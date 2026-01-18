# 🛍️ Hệ thống Phân loại Cảm xúc Đánh giá Sản phẩm Shopee

## 📋 Tổng quan dự án

Đồ án phân tích sắc thái ngôn từ (Sentiment Analysis) cho các bình luận đánh giá sản phẩm trên nền tảng Shopee. Dự án sử dụng kiến trúc Big Data với Apache Kafka và Apache Spark để xử lý dữ liệu streaming theo thời gian thực, kết hợp với các mô hình Machine Learning để phân loại cảm xúc của khách hàng.

### 🎯 Mục tiêu

- Thu thập dữ liệu đánh giá sản phẩm từ Shopee API
- Xử lý dữ liệu streaming theo thời gian thực với Apache Kafka và PySpark
- Tiền xử lý văn bản tiếng Việt (loại bỏ stopwords, emoji, ký tự đặc biệt)
- Phân loại cảm xúc (tích cực/tiêu cực/trung lập) của bình luận khách hàng
- Lưu trữ và phân tích kết quả

### 🏗️ Kiến trúc hệ thống

```
Shopee API → Producer (Kafka) → rawData Topic → Streaming Processing (PySpark) → resultData Topic → Consumer
                                                          ↓
                                                  ML Model (Sentiment Analysis)
                                                          ↓
                                                  Text Preprocessing
```

## 🚀 Công nghệ sử dụng

### Core Technologies

- **Apache Kafka**: Message broker để xử lý dữ liệu streaming
- **Apache Spark (PySpark)**: Framework xử lý dữ liệu phân tán
- **Python**: Ngôn ngữ lập trình chính

### Machine Learning & NLP

- **Scikit-learn**: Framework machine learning
- **PyVi**: Tokenization cho tiếng Việt
- **NLTK**: Natural Language Processing toolkit

### Libraries

- `kafka-python`: Kafka client cho Python
- `pyspark`: Apache Spark Python API
- `numpy`, `pandas`: Xử lý dữ liệu
- `regex`, `emot`: Xử lý text và emoji
- `joblib`: Serialize/deserialize models

## 📁 Cấu trúc dự án

```
Final_project/
├── config/                          # Cấu hình hệ thống
│   ├── __init__.py
│   └── kafka_config.py             # Cấu hình Kafka broker và topics
├── data/                           # Dữ liệu và tài nguyên
│   ├── shopee_data_raw.csv        # Dữ liệu thô từ Shopee
│   ├── dictionary/                # Từ điển viết tắt, emoji
│   │   ├── abb_dict_normal.xlsx
│   │   ├── abb_dict_special.xlsx
│   │   ├── emoji2word.xlsx
│   │   └── character2emoji.xlsx
│   ├── model/                     # Mô hình ML đã train
│   ├── vietnamese_stop_word/      # Danh sách stopwords tiếng Việt
│   │   ├── vietnamese-stopwords.txt
│   │   └── vietnamese-stopwords-dash.txt
│   └── vihsd_dataset/            # Dataset huấn luyện
│       ├── train.csv
│       ├── dev.csv
│       └── test.csv
├── notebooks/                     # Jupyter notebooks
│   ├── crawldata.ipynb           # Thu thập dữ liệu
│   ├── data_processing.ipynb     # Xử lý dữ liệu
│   ├── model_trainning.ipynb     # Huấn luyện mô hình
│   └── IE212_project.ipynb       # Notebook chính của dự án
├── checkpoints/                   # Spark streaming checkpoints
│   └── processed_feedback/
├── producer.py                    # Kafka Producer - Thu thập dữ liệu từ Shopee
├── streaming.py                   # Spark Streaming - Xử lý real-time
├── consumer.py                    # Kafka Consumer - Nhận kết quả
├── data_processing.py            # Các hàm tiền xử lý văn bản
├── requirements.txt              # Dependencies
└── README.md                     # Tài liệu dự án
```

## 📦 Cài đặt

### Yêu cầu hệ thống

- Python 3.8+
- Apache Kafka 2.x hoặc 3.x
- Java 8 hoặc 11 (cho Spark)
- 4GB RAM trở lên

### Bước 1: Clone repository

```bash
git clone https://github.com/tdminle/IE212.P11-BigData.git
cd Final_project
```

### Bước 2: Tạo môi trường ảo (khuyến nghị)

```bash
# Windows
python -m venv myenv
myenv\Scripts\activate

# Linux/Mac
python -m venv myenv
source myenv/bin/activate
```

### Bước 3: Cài đặt dependencies

```bash
pip install -r requirements.txt
```

### Bước 4: Cài đặt và khởi động Kafka

1. **Download Apache Kafka**: https://kafka.apache.org/downloads
2. **Giải nén và di chuyển vào thư mục Kafka**

```bash
# Khởi động Zookeeper
bin/zookeeper-server-start.sh config/zookeeper.properties  # Linux/Mac
bin\windows\zookeeper-server-start.bat config\zookeeper.properties  # Windows

# Khởi động Kafka broker (terminal mới)
bin/kafka-server-start.sh config/server.properties  # Linux/Mac
bin\windows\kafka-server-start.bat config\server.properties  # Windows
```

### Bước 5: Tạo Kafka Topics

```bash
# Tạo topic rawData
kafka-topics --create --topic rawData --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

# Tạo topic resultData
kafka-topics --create --topic resultData --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

# Kiểm tra topics đã tạo
kafka-topics --list --bootstrap-server localhost:9092
```

## 🎯 Sử dụng

### Quy trình hoạt động

1. **Producer** thu thập dữ liệu đánh giá từ Shopee API và gửi vào topic `rawData`
2. **Streaming** đọc dữ liệu từ `rawData`, tiền xử lý và phân loại cảm xúc, sau đó gửi kết quả vào topic `resultData`
3. **Consumer** nhận kết quả từ `resultData` để lưu trữ hoặc hiển thị

### Chạy hệ thống

**Terminal 1 - Khởi động Spark Streaming:**

```bash
python streaming.py
```

**Terminal 2 - Khởi động Consumer:**

```bash
python consumer.py
```

**Terminal 3 - Khởi động Producer:**

```bash
python producer.py
```

### Huấn luyện mô hình (Tùy chọn)

Mở và chạy notebook `IE212_project.ipynb` hoặc `model_trainning.ipynb` để huấn luyện lại mô hình với dữ liệu mới.

## ⚙️ Cấu hình

### Kafka Configuration (`config/kafka_config.py`)

```python
KAFKA_BROKER = "localhost:9092"      # Địa chỉ Kafka broker
TOPIC_RAW_DATA = "rawData"           # Topic cho dữ liệu thô
TOPIC_RESULT_DATA = "resultData"     # Topic cho kết quả phân loại
```

### Shopee API Configuration (`producer.py`)

```python
item_id = "YOUR_ITEM_ID"      # ID sản phẩm trên Shopee
shop_id = "YOUR_SHOP_ID"      # ID cửa hàng trên Shopee
limit = 10                    # Số lượng bình luận mỗi lần fetch
offset = 0                    # Offset để phân trang
```

## 🔧 Chi tiết các module

### 1. Producer (`producer.py`)

- Kết nối với Shopee API để lấy đánh giá sản phẩm
- Serialize dữ liệu thành JSON
- Gửi dữ liệu vào Kafka topic `rawData`
- Hỗ trợ streaming liên tục với offset

### 2. Streaming (`streaming.py`)

- Đọc dữ liệu streaming từ topic `rawData` bằng Spark Structured Streaming
- Áp dụng UDF (User Defined Function) để:
  - Tiền xử lý văn bản (preprocessing)
  - Dự đoán cảm xúc (sentiment prediction)
- Gửi kết quả vào topic `resultData`
- Sử dụng checkpoint để đảm bảo fault-tolerance

### 3. Consumer (`consumer.py`)

- Nhận kết quả phân loại từ topic `resultData`
- Xử lý và lưu trữ kết quả
- Hỗ trợ tích hợp với PySpark để xử lý batch

### 4. Data Processing (`data_processing.py`)

Bao gồm các hàm tiền xử lý văn bản tiếng Việt:

- `filter_stop_words()`: Loại bỏ stopwords
- `remove_emoji()`: Xóa emoji
- `url()`: Loại bỏ URL
- `special_character()`: Xóa ký tự đặc biệt
- `repeated_character()`: Chuẩn hóa ký tự lặp
- `mail()`: Loại bỏ email
- `tag()`: Xóa mention và hashtag
- `convert_character2emoji()`: Chuyển đổi ký tự thành emoji
- `preprocessing()`: Pipeline xử lý hoàn chỉnh

## 📊 Dataset

### VIHSD Dataset

Dataset huấn luyện sử dụng **ViHSD** (Vietnamese Hate Speech Detection):

- `train.csv`: Dữ liệu huấn luyện
- `dev.csv`: Dữ liệu validation
- `test.csv`: Dữ liệu kiểm tra

### Shopee Data

Dữ liệu thu thập từ Shopee API bao gồm:

- Rating (1-5 sao)
- Comment (nội dung đánh giá)
- Order ID
- Timestamp

## 🧪 Testing

```bash
# Test Kafka connection
kafka-console-consumer --bootstrap-server localhost:9092 --topic rawData --from-beginning

# Test Producer
python producer.py

# Test Consumer
python consumer.py
```

## 📈 Performance & Optimization

- **Checkpointing**: Spark Streaming sử dụng checkpoint để đảm bảo exactly-once processing
- **Batch Processing**: Consumer có thể xử lý theo batch với PySpark
- **Parallel Processing**: Kafka partitions cho phép xử lý song song
- **Caching**: Các dictionary và stopwords được load một lần vào memory

## 🐛 Troubleshooting

### Kafka không khởi động được

- Kiểm tra Zookeeper đã chạy chưa
- Kiểm tra port 9092 có bị chiếm không

### Spark Streaming lỗi

- Kiểm tra Java version (cần Java 8 hoặc 11)
- Kiểm tra checkpoint directory có quyền write không
- Xóa checkpoint cũ nếu schema thay đổi

### Producer không lấy được dữ liệu

- Kiểm tra `item_id` và `shop_id` có đúng không
- Kiểm tra kết nối internet
- Shopee API có thể có rate limit

## 👥 Contributors

- Đồ án môn học IE212.P11 - Big Data
- Repository: https://github.com/tdminle/IE212.P11-BigData

## 📝 License

Dự án này được phát triển cho mục đích học tập và nghiên cứu.

## 📞 Contact

Nếu có vấn đề hoặc câu hỏi, vui lòng tạo issue trên GitHub repository.

---

**Lưu ý**: Đảm bảo tuân thủ Terms of Service của Shopee khi sử dụng API để thu thập dữ liệu.
