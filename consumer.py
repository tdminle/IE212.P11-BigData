"""
Consumer module để nhận và xử lý kết quả phân loại cảm xúc từ Kafka
Sử dụng Kafka Consumer và PySpark để xử lý dữ liệu
"""

from kafka import KafkaConsumer
from kafka.errors import KafkaError
import json
import pandas as pd
import logging

import findspark
findspark.init()

from pyspark.sql.types import StructType, StructField, LongType, IntegerType, StringType
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import col

# Cấu hình logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Cấu hình Spark
scala_version = '2.12'
spark_version = '3.5.3'

packages = [
    f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}',
    'org.apache.kafka:kafka-clients:3.9.0'
]

# Khởi tạo Spark Session với cấu hình tối ưu
spark = None

def init_spark():
    """
    Khởi tạo Spark Session
    Returns:
        SparkSession: Spark session instance hoặc None nếu lỗi
    """
    global spark
    try:
        spark = (SparkSession.builder
                .master('local')
                .appName('streaming_data_consumer')
                .config('spark.jars.packages', ",".join(packages))
                .config('spark.sql.streaming.checkpointLocation', 'checkpoints/consumer')
                .getOrCreate())
        
        # Giảm log level để tránh spam
        spark.sparkContext.setLogLevel("WARN")
        logger.info("Spark session đã được khởi tạo thành công")
        return spark
    except Exception as e:
        logger.error(f"Lỗi khi khởi tạo Spark: {e}")
        return None

def create_consumer(topic, group_id='consumer_group_1'):
    """
    Tạo KafkaConsumer để nhận dữ liệu từ một Kafka topic
    
    Args:
        topic (str): Tên topic cần subscribe
        group_id (str): Consumer group ID
    
    Returns:
        KafkaConsumer: Consumer instance hoặc None nếu lỗi
    """
    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=['localhost:9092'],  # Kafka broker
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),  # Chuyển đổi dữ liệu JSON
            auto_offset_reset='latest',  # Đảm bảo chỉ lấy dữ liệu mới
            enable_auto_commit=True,     # Tự động commit offset
            auto_commit_interval_ms=1000,  # Commit mỗi 1 giây
            group_id=group_id,           # Consumer group
            max_poll_records=100,        # Số lượng records tối đa mỗi lần poll
            session_timeout_ms=30000,    # Timeout session
            heartbeat_interval_ms=3000   # Heartbeat interval
        )
        logger.info(f"Kafka consumer đã được khởi tạo cho topic: {topic}")
        return consumer
    except KafkaError as e:
        logger.error(f"Lỗi khi khởi tạo Kafka consumer: {e}")
        return None
    except Exception as e:
        logger.error(f"Lỗi không xác định khi tạo consumer: {e}")
        return None


def consume_data(consumer):
    """
    Tiêu thụ dữ liệu từ Kafka và xử lý bằng Spark DataFrame
    
    Args:
        consumer (KafkaConsumer): Kafka consumer instance
    """
    if consumer is None:
        logger.error("Consumer chưa được khởi tạo")
        return
    
    if spark is None:
        logger.error("Spark session chưa được khởi tạo")
        return
    
    logger.info("Đang chờ dữ liệu từ Kafka...")
    message_count = 0
    
    try:
        for message in consumer:
            try:
                message_count += 1
                logger.info(f"[Message #{message_count}] Received from partition {message.partition}, offset {message.offset}")
                
                # Validate message value
                message_data = message.value
                if not message_data:
                    logger.warning("Nhận được message rỗng, bỏ qua...")
                    continue
                
                logger.info(f"Message data: {message_data}")
                
                # Chuyển dictionary thành Row để tạo Spark DataFrame
                row = Row(**message_data)
                
                # Tạo DataFrame từ Row (FIX: Tránh tạo DataFrame mới liên tục - có thể gây memory leak)
                df = spark.createDataFrame([row])
                
                # Hiển thị DataFrame
                df.show(truncate=False)
                
                # TODO: Lưu vào database hoặc file nếu cần
                # df.write.mode('append').parquet('output/results')
                
            except Exception as e:
                logger.error(f"Lỗi khi xử lý message: {e}")
                continue  # Tiếp tục xử lý message tiếp theo
                
    except KeyboardInterrupt:
        logger.info("Nhận tín hiệu ngắt (Ctrl+C). Đang dừng consumer...")
    except Exception as e:
        logger.error(f"Lỗi không xác định trong consume loop: {e}")
    finally:
        # Cleanup
        if consumer:
            try:
                consumer.close()
                logger.info("Đã đóng Kafka consumer")
            except Exception as e:
                logger.error(f"Lỗi khi đóng consumer: {e}")

def main():
    """
    Hàm chính để chạy consumer
    """
    # Khởi tạo Spark session
    if init_spark() is None:
        logger.error("Không thể khởi tạo Spark. Thoát chương trình.")
        return
    
    # Tên topic cần consume
    topic = "resultData"  # Topic chứa kết quả phân loại cảm xúc
    
    # Tạo consumer
    consumer = create_consumer(topic)
    if consumer is None:
        logger.error("Không thể tạo Kafka consumer. Thoát chương trình.")
        # Cleanup Spark
        if spark:
            spark.stop()
        return
    
    logger.info(f"Consumer đã sẵn sàng. Đang lắng nghe topic '{topic}'...")
    
    try:
        # Bắt đầu consume dữ liệu
        consume_data(consumer)
    finally:
        # Cleanup Spark session
        if spark:
            try:
                spark.stop()
                logger.info("Đã dừng Spark session")
            except Exception as e:
                logger.error(f"Lỗi khi dừng Spark: {e}")

if __name__ == "__main__":
    main()

