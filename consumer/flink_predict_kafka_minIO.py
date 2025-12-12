import os
import json
import requests
import logging
from datetime import datetime  # <--- THÊM: Để xử lý thời gian cho Partition

from pyflink.common import Types, Time, WatermarkStrategy, Configuration, Row
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.functions import ProcessWindowFunction
from pyflink.datastream.window import TumblingProcessingTimeWindows
from pyflink.common.serialization import SimpleStringSchema

# --- IMPORT SQL & EXPRESSIONS ---
from pyflink.table import StreamTableEnvironment, DataTypes
from pyflink.table.expressions import col

# --- CẤU HÌNH ---
KAFKA_BROKERS = "localhost:9092"
INPUT_TOPIC = "iot-traffic-congestion"
OUTPUT_TOPIC = "traffic-predictions"
MINIO_BUCKET = "traffic-data"
API_URL = "http://127.0.0.1:8127/predict" 

# Tên file JARs (Đảm bảo file nằm cùng thư mục code)
KAFKA_JAR = "flink-sql-connector-kafka-3.0.0-1.17.jar"
S3_JAR = "flink-s3-fs-hadoop-1.17.0.jar"

class PredictionWindowProcess(ProcessWindowFunction):
    def process(self, key, context, elements):
        sensor_id = key
        # Sắp xếp theo thời gian
        sorted_elements = sorted(elements, key=lambda x: x['timestamp'])
        current_len = len(sorted_elements)
        
        target_records = []

        # Logic padding dữ liệu (giữ nguyên của bạn)
        if current_len < 3:
            return 
        elif current_len < 5:
            missing = 5 - current_len
            last_item = sorted_elements[-1]
            target_records = sorted_elements + [last_item] * missing
        else:
            target_records = sorted_elements[-5:]

        data_batch = []
        for row in target_records:
            feature_row = [
                row['traffic_volume'],
                row['avg_vehicle_speed'],
                row['vehicle_breakdown']['cars'],
                row['vehicle_breakdown']['trucks'],
                row['vehicle_breakdown']['bikes']
            ]
            data_batch.append(feature_row)

        try:
            # Gọi API
            headers = {'Content-Type': 'application/json', 'accept': 'application/json'}
            response = requests.post(API_URL, json={"data": data_batch}, headers=headers, timeout=1)
            
            if response.status_code == 200:
                result = response.json()
                pred_label = result.get('prediction', -1)
                
                status_map = {0: "Thông thoáng", 1: "Bình thường", 2: "TẮC NGHẼN"}
                status_text = status_map.get(pred_label, "Unknown")
                
                # Lấy timestamp của bản ghi cuối cùng để làm mốc phân chia thư mục
                last_ts = target_records[-1]['timestamp']
                dt_obj = datetime.fromtimestamp(last_ts)
                
                # Tạo các biến Partition (Dạng String)
                year_str = str(dt_obj.year)
                month_str = str(dt_obj.month).zfill(2) # 01, 02...
                day_str = str(dt_obj.day).zfill(2)

                output_message = {
                    "sensor_id": sensor_id,
                    "window_end": str(context.window().end),
                    "prediction": status_text,
                    "label": pred_label,
                    "timestamp": last_ts
                }
                
                print(f"✅ Prediction (Key={sensor_id}): {status_text}")
                
                # --- THAY ĐỔI: Trả về Row chứa 5 trường ---
                # 1. sensor_id (cho Kafka Key)
                # 2. msg (payload JSON)
                # 3. year, 4. month, 5. day (cho MinIO Partition)
                yield Row(sensor_id, json.dumps(output_message), year_str, month_str, day_str)
                
            else:
                print(f"❌ API Error: {response.status_code}")
                
        except Exception as e:
            print(f"❌ Exception: {str(e)}")

def main():
    current_dir = os.getcwd()
    kafka_jar_path = f"file://{os.path.join(current_dir, KAFKA_JAR)}"
    s3_jar_path = f"file://{os.path.join(current_dir, S3_JAR)}"

    print(f"🔄 Loading JARs...")

    config = Configuration()
    config.set_string("rest.port", "8081")
    config.set_string("rest.address", "localhost")
    # Load cả 2 JAR
    config.set_string("pipeline.jars", f"{kafka_jar_path};{s3_jar_path}")
    
    # --- CẤU HÌNH MINIO ---
    config.set_string("s3.endpoint", "http://localhost:9000")
    config.set_string("s3.path.style.access", "true")
    config.set_string("s3.access-key", "minioadmin")
    config.set_string("s3.secret-key", "minioadmin")
    
    env = StreamExecutionEnvironment.get_execution_environment(config)
    
    # --- CẤU HÌNH CHECKPOINT ---
    # Đặt 60s (1 phút) để cân bằng hiệu năng vì data không quá nhiều
    env.enable_checkpointing(60000) 
    
    t_env = StreamTableEnvironment.create(env)

    print(f"🚀 Job Started. Checkpoint: 60s. Rolling File: 15 min.")

    source = KafkaSource.builder() \
        .set_bootstrap_servers(KAFKA_BROKERS) \
        .set_topics(INPUT_TOPIC) \
        .set_group_id("flink-traffic-group-optimized-v3") \
        .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    ds = env.from_source(source, WatermarkStrategy.no_watermarks(), "Kafka Source")

    processed_ds = ds \
        .map(lambda x: json.loads(x), output_type=Types.PICKLED_BYTE_ARRAY()) \
        .key_by(lambda x: x['sensor_id']) \
        .window(TumblingProcessingTimeWindows.of(Time.seconds(28))) \
        .process(PredictionWindowProcess()) \
        .map(lambda x: x, output_type=Types.ROW([
            Types.STRING(), # sensor_id
            Types.STRING(), # msg
            Types.STRING(), # year
            Types.STRING(), # month
            Types.STRING()  # day
        ])) 

    # Chuyển DataStream thành Table (có 5 cột)
    # Đặt tên cột rõ ràng để map vào SQL
    source_table = t_env.from_data_stream(
        processed_ds, 
        col("sensor_id"), 
        col("msg"), 
        col("y"), 
        col("m"), 
        col("d")
    )

    # 1. Tạo bảng Kafka Sink (Chỉ cần sensor_id và msg)
    t_env.execute_sql(f"""
        CREATE TABLE KafkaSinkTable (
            sensor_id STRING,
            msg STRING
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{OUTPUT_TOPIC}',
            'properties.bootstrap.servers' = '{KAFKA_BROKERS}',
            'key.format' = 'raw',
            'key.fields' = 'sensor_id',
            'value.format' = 'raw',
            'value.fields-include' = 'EXCEPT_KEY' 
        )
    """)

    # 2. Tạo bảng MinIO Sink (Có Partition + Rolling Policy)
    t_env.execute_sql(f"""
        CREATE TABLE MinIOSinkTable (
            sensor_id STRING,
            msg STRING,
            `y` STRING,
            `m` STRING,
            `d` STRING
        ) PARTITIONED BY (`y`, `m`, `d`) -- Chia thư mục theo năm/tháng/ngày
        WITH (
            'connector' = 'filesystem',
            'path' = 's3a://{MINIO_BUCKET}/predictions/',
            'format' = 'json',
            
            -- Cấu hình gom file (Rolling Policy)
            'sink.rolling-policy.file-size' = '128MB',       -- File quá 128MB thì ngắt
            'sink.rolling-policy.rollover-interval' = '15 min', -- Hoặc quá 15 phút thì ngắt
            'sink.rolling-policy.check-interval' = '1 min'   -- Check mỗi phút
        )
    """)

    # --- SỬ DỤNG STATEMENT SET ĐỂ CHẠY CẢ 2 SINK ---
    statement_set = t_env.create_statement_set()
    
    # Sink 1: Vào Kafka (Chọn đúng 2 cột cần thiết)
    statement_set.add_insert("KafkaSinkTable", source_table.select(col("sensor_id"), col("msg")))
    
    # Sink 2: Vào MinIO (Lấy hết cả 5 cột để Partition hoạt động)
    statement_set.add_insert("MinIOSinkTable", source_table)
    
    print("⏳ Executing sinks...")
    statement_set.execute().wait()

if __name__ == '__main__':
    logging.basicConfig(level=logging.ERROR)
    os.environ['JAVA_TOOL_OPTIONS'] = "--add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED"
    main()