import os
import json
import requests
import logging
from pyflink.common import Types, Time, WatermarkStrategy, Configuration, Row  # <--- THÊM Row
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
API_URL = "http://127.0.0.1:8127/predict" 
JAR_FILE_NAME = "flink-sql-connector-kafka-3.0.0-1.17.jar"

class PredictionWindowProcess(ProcessWindowFunction):
    def process(self, key, context, elements):
        sensor_id = key
        sorted_elements = sorted(elements, key=lambda x: x['timestamp'])
        current_len = len(sorted_elements)
        
        target_records = []

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
            headers = {'Content-Type': 'application/json', 'accept': 'application/json'}
            response = requests.post(API_URL, json={"data": data_batch}, headers=headers, timeout=1)
            
            if response.status_code == 200:
                result = response.json()
                pred_label = result.get('prediction', -1)
                
                status_map = {0: "Thông thoáng", 1: "Bình thường", 2: "TẮC NGHẼN"}
                status_text = status_map.get(pred_label, "Unknown")

                output_message = {
                    "sensor_id": sensor_id,
                    "window_end": str(context.window().end),
                    "prediction": status_text,
                    "label": pred_label,
                    "volume": data_batch[-1][0],
                    "timestamp": target_records[-1]['timestamp']
                }
                
                print(f"✅ Kafka Sink (Key={sensor_id}): {status_text}")
                
                # --- SỬA LỖI Ở ĐÂY: Dùng Row thay vì Tuple ---
                yield Row(sensor_id, json.dumps(output_message))
                
            else:
                print(f"❌ API Error: {response.status_code}")
                
        except Exception as e:
            print(f"❌ Exception: {str(e)}")

def main():
    current_dir = os.getcwd()
    jar_path = f"file://{os.path.join(current_dir, JAR_FILE_NAME)}"
    print(f"🔄 Loading JAR: {jar_path}")

    config = Configuration()
    config.set_string("rest.port", "8081")
    config.set_string("rest.address", "localhost")
    config.set_string("pipeline.jars", jar_path) 
    
    env = StreamExecutionEnvironment.get_execution_environment(config)
    t_env = StreamTableEnvironment.create(env)

    print(f"🚀 Flink Job Started... Reading: {INPUT_TOPIC} -> Writing: {OUTPUT_TOPIC}")

    source = KafkaSource.builder() \
        .set_bootstrap_servers(KAFKA_BROKERS) \
        .set_topics(INPUT_TOPIC) \
        .set_group_id("flink-traffic-sink-group-sql-keyed-v2") \
        .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    ds = env.from_source(source, WatermarkStrategy.no_watermarks(), "Kafka Source")

    processed_ds = ds \
        .map(lambda x: json.loads(x), output_type=Types.PICKLED_BYTE_ARRAY()) \
        .key_by(lambda x: x['sensor_id']) \
        .window(TumblingProcessingTimeWindows.of(Time.seconds(28))) \
        .process(PredictionWindowProcess()) \
        .map(lambda x: x, output_type=Types.ROW([Types.STRING(), Types.STRING()])) 

    # Chuyển DataStream sang Table
    table = t_env.from_data_stream(processed_ds, col("sensor_id"), col("msg"))

    # Cấu hình Table Sink có Key
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

    table.execute_insert("KafkaSinkTable").wait()

if __name__ == '__main__':
    logging.basicConfig(level=logging.ERROR)
    os.environ['JAVA_TOOL_OPTIONS'] = "--add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED"
    main()