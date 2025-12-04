import os
import json
import requests
import logging
from pyflink.common import Types, Time, WatermarkStrategy, Configuration
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.functions import ProcessWindowFunction
from pyflink.datastream.window import TumblingProcessingTimeWindows
from pyflink.common.serialization import SimpleStringSchema

# --- CẤU HÌNH ---
KAFKA_BROKERS = "localhost:9092"
TOPIC_NAME = "iot-traffic-congestion"
# Thay bằng IP thật máy bạn (đã tìm được ở bước trước)
API_URL = "http://127.0.0.1:8127/predict" 
JAR_FILE_NAME = "flink-sql-connector-kafka-3.0.0-1.17.jar"

class PredictionWindowProcess(ProcessWindowFunction):
    """
    Logic nâng cao:
    1. Tự động Padding (điền khuyết) nếu thiếu dữ liệu.
    2. Tự động cắt bớt nếu thừa dữ liệu.
    3. Yield kết quả thay vì Print để tránh lỗi NoneType.
    """
    def process(self, key, context, elements):
        sensor_id = key
        
        # 1. Sắp xếp theo thời gian
        sorted_elements = sorted(elements, key=lambda x: x['timestamp'])
        current_len = len(sorted_elements)
        
        target_records = []

        # --- LOGIC CHỐNG MẤT DỮ LIỆU ---
        
        # Trường hợp 1: Quá ít dữ liệu (ví dụ sensor mới khởi động) -> Bỏ qua
        if current_len < 3:
            yield f"⚠️ [SKIP] {sensor_id}: Dữ liệu quá ít ({current_len}/5) -> Không thể Padding."
            return

        # Trường hợp 2: Thiếu 1-2 bản tin (Mạng lag) -> PADDING (Nhân bản dòng cuối)
        elif current_len < 5:
            missing = 5 - current_len
            last_item = sorted_elements[-1]
            
            # Copy dòng cuối cùng lấp vào chỗ trống
            padded_elements = sorted_elements + [last_item] * missing
            target_records = padded_elements
            # yield f"ℹ️ [INFO] {sensor_id}: Đã fix dữ liệu (Gốc: {current_len} -> Padding: 5)"

        # Trường hợp 3: Đủ hoặc Thừa (Do mở rộng Window) -> Lấy 5 dòng mới nhất
        else:
            target_records = sorted_elements[-5:]

        # -------------------------------

        # 2. Trích xuất Feature (Input cho Model)
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

        # 3. Gọi API (Có Timeout để không treo Flink)
        payload = {"data": data_batch}
        
        try:
            headers = {'Content-Type': 'application/json', 'accept': 'application/json'}
            # Timeout cực quan trọng: Nếu API treo quá 1s, Flink sẽ bỏ qua để xử lý cái khác
            response = requests.post(API_URL, json=payload, headers=headers, timeout=1)
            
            if response.status_code == 200:
                result = response.json()
                pred = result.get('prediction', 'Unknown')
                
                # Mapping kết quả cho dễ hiểu
                status_map = {0: "🟢 Thông thoáng", 1: "🟡 Bình thường", 2: "🔴 TẮC NGHẼN"}
                status_text = status_map.get(pred, f"Label {pred}")

                yield (
                    f"🔮 {sensor_id} | "
                    f"Win: {current_len} recs | "
                    f"InputVol: {data_batch[-1][0]} | "
                    f"👉 {status_text}"
                )
            else:
                yield f"❌ API Error {response.status_code}"
                
        except Exception as e:
            yield f"❌ Lỗi kết nối API: {str(e)}"

def main():
    # Cấu hình Web UI
    config = Configuration()
    config.set_string("rest.port", "8081")
    config.set_string("rest.address", "localhost")
    env = StreamExecutionEnvironment.get_execution_environment(config)
    
    # Load JAR
    current_dir = os.getcwd()
    jar_path = f"file://{os.path.join(current_dir, JAR_FILE_NAME)}"
    print(f"🔄 Loading JAR: {jar_path}")
    try:
        env.add_jars(jar_path)
    except:
        return

    print("🚀 Flink Job Started... (Web UI: http://localhost:8081)")

    source = KafkaSource.builder() \
        .set_bootstrap_servers(KAFKA_BROKERS) \
        .set_topics(TOPIC_NAME) \
        .set_group_id("flink-traffic-group-optimized") \
        .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    ds = env.from_source(source, WatermarkStrategy.no_watermarks(), "Kafka Source")

    ds \
        .map(lambda x: json.loads(x), output_type=Types.PICKLED_BYTE_ARRAY()) \
        .key_by(lambda x: x['sensor_id']) \
        .window(TumblingProcessingTimeWindows.of(Time.seconds(28))) \
        .process(PredictionWindowProcess()) \
        .print()
    env.execute("Traffic Prediction Job (Robust)")

if __name__ == '__main__':
    logging.basicConfig(level=logging.ERROR)
    main()