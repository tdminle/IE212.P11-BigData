"""
Streaming module để xử lý dữ liệu real-time từ Kafka
Sử dụng Spark Structured Streaming để đọc, xử lý và ghi dữ liệu
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql import functions as f
from pyspark.sql.types import StringType, IntegerType, StructType, StructField
from config.kafka_config import KAFKA_BROKER, TOPIC_RAW_DATA, TOPIC_RESULT_DATA
import json
from data_processing import preprocessing
import findspark
import logging

findspark.init()

# Cấu hình logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Cấu hình Spark packages
scala_version = '2.12'
spark_version = '3.5.3'

packages = [
    f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}',
    'org.apache.kafka:kafka-clients:3.9.0'
]

# Khởi tạo Spark Session với cấu hình tối ưu
try:
    spark = (SparkSession.builder
            .master('local[*]')  # Sử dụng tất cả cores
            .appName('streaming_sentiment_analysis')
            .config('spark.jars.packages', ",".join(packages))
            .config('spark.sql.streaming.checkpointLocation', 'checkpoints/streaming')
            .config('spark.sql.streaming.schemaInference', 'true')
            .getOrCreate())
    
    # Giảm log level
    spark.sparkContext.setLogLevel("WARN")
    logger.info("Spark session đã được khởi tạo thành công")
except Exception as e:
    logger.error(f"Lỗi khi khởi tạo Spark: {e}")
    raise

# Hàm phân loại cảm xúc (placeholder - nên thay bằng model thực tế)
def sentiment_predict(comment):
    """
    Dự đoán cảm xúc từ comment (placeholder)
    TODO: Tích hợp mô hình ML thực tế từ file model/
    
    Args:
        comment (str): Comment đã được tiền xử lý
    
    Returns:
        int: 1 cho positive, 0 cho negative, -1 cho lỗi
    """
    try:
        # CẢNH BÁO: Đây chỉ là logic đơn giản, cần thay bằng model thực tế
        if comment is None or comment.strip() == "":
            return -1  # Unknown/Error
        
        # Danh sách từ tích cực và tiêu cực đơn giản
        positive_words = ["tốt", "đẹp", "chất_lượng", "hài_lòng", "tuyệt", "ok", "oke"]
        negative_words = ["tệ", "kém", "xấu", "không_tốt", "thất_vọng", "dở"]
        
        comment_lower = comment.lower()
        
        # Đếm số từ tích cực và tiêu cực
        pos_count = sum(1 for word in positive_words if word in comment_lower)
        neg_count = sum(1 for word in negative_words if word in comment_lower)
        
        if pos_count > neg_count:
            return 1  # Positive
        elif neg_count > pos_count:
            return 0  # Negative
        else:
            return 1  # Neutral -> Positive (mặc định)
            
    except Exception as e:
        logger.error(f"Lỗi trong sentiment_predict: {e}")
        return -1  # Error case

# Đăng ký UDF (User Defined Function) với error handling
try:
    preprocess_udf = udf(preprocessing, StringType())
    sentiment_udf = udf(sentiment_predict, IntegerType())
    logger.info("UDFs đã được đăng ký thành công")
except Exception as e:
    logger.error(f"Lỗi khi đăng ký UDF: {e}")
    raise


# Đọc dữ liệu stream từ Kafka topic rawData
try:
    logger.info(f"Đang kết nối đến Kafka broker: {KAFKA_BROKER}")
    logger.info(f"Subscribe topic: {TOPIC_RAW_DATA}")
    
    feedback_stream = (spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", TOPIC_RAW_DATA)
        .option("failOnDataLoss", "false")  # Không fail nếu mất dữ liệu (cho development)
        .option("startingOffsets", "latest")  # Đọc từ offset mới nhất
        .load())
    
    logger.info("Đọc stream từ Kafka thành công!")
    
except Exception as e:
    logger.error(f"Lỗi khi đọc stream từ Kafka: {e}")
    raise

# Định nghĩa schema cho dữ liệu (FIX: orderID -> orderid để khớp với producer)
schema_value = StructType([
    StructField("rating", StringType(), True),
    StructField("comment", StringType(), True),
    StructField("orderid", StringType(), True),  # FIX: Sửa từ orderID thành orderid
    StructField("time", StringType(), True),
])

# Parse JSON từ Kafka value
try:
    df_json = (feedback_stream
               .selectExpr("CAST(value AS STRING)")
               .withColumn("value", f.from_json("value", schema_value)))
    
    logger.info("Schema của DataFrame:")
    df_json.printSchema()
    
except Exception as e:
    logger.error(f"Lỗi khi parse JSON: {e}")
    raise

# Tiền xử lý dữ liệu với null handling
try:
    # FIX: Thêm coalesce để xử lý null values
    processed_stream = df_json.withColumn(
        "processed_comment", 
        preprocess_udf(f.coalesce(col("value.comment"), f.lit("")))
    )
    
    logger.info("Đã áp dụng preprocessing UDF")
    
except Exception as e:
    logger.error(f"Lỗi khi tiền xử lý: {e}")
    raise

# Dự đoán cảm xúc
try:
    predictions = processed_stream.withColumn(
        "sentiment", 
        sentiment_udf(col("processed_comment"))
    )
    
    logger.info("Đã áp dụng sentiment prediction UDF")
    
except Exception as e:
    logger.error(f"Lỗi khi dự đoán cảm xúc: {e}")
    raise

# Chuyển đổi dữ liệu thành định dạng JSON để gửi lại vào Kafka
try:
    # Chọn các cột cần thiết và tạo JSON
    result_data = predictions.select(
        col("value.rating").alias("rating"),  # Thêm thông tin rating
        col("value.comment").alias("original_comment"),  # Comment gốc
        col("processed_comment"),
        col("sentiment"),
        col("value.time").alias("time")  # Thêm timestamp
    ).withColumn(
        "value", 
        f.to_json(f.struct("rating", "original_comment", "processed_comment", "sentiment", "time"))
    ).select("value")  # Chỉ lấy cột value để ghi vào Kafka
    
    logger.info("Đã chuẩn bị dữ liệu để gửi vào Kafka")
    
except Exception as e:
    logger.error(f"Lỗi khi chuẩn bị dữ liệu output: {e}")
    raise

# Gửi kết quả vào Kafka topic result_data
try:
    logger.info(f"Bắt đầu ghi stream vào topic: {TOPIC_RESULT_DATA}")
    
    query = (result_data.writeStream
        .outputMode("append")
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("topic", TOPIC_RESULT_DATA)
        .option("checkpointLocation", "checkpoints/processed_feedback")
        .start())
    
    logger.info("Stream processing đã bắt đầu thành công!")
    logger.info("Đang xử lý dữ liệu... (Ctrl+C để dừng)")
    
    # Chờ cho đến khi có dữ liệu và tiếp tục xử lý
    query.awaitTermination()
    
except KeyboardInterrupt:
    logger.info("Nhận tín hiệu ngắt (Ctrl+C). Đang dừng stream...")
    if 'query' in locals():
        query.stop()
    logger.info("Stream đã dừng")
except Exception as e:
    logger.error(f"Lỗi trong quá trình streaming: {e}")
    if 'query' in locals():
        query.stop()
    raise
finally:
    # Cleanup
    if spark:
        try:
            spark.stop()
            logger.info("Đã dừng Spark session")
        except Exception as e:
            logger.error(f"Lỗi khi dừng Spark: {e}")

