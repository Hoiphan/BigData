import time
import json
import csv
import os
from kafka import KafkaProducer
from datetime import datetime
from itertools import groupby

# 1. Cấu hình Kafka Producer
PRODUCER_CONF = {
    'bootstrap_servers': ['localhost:9092'],
    'value_serializer': lambda x: json.dumps(x).encode('utf-8'),
    'key_serializer': lambda k: k.encode('utf-8')
}

TOPIC_NAME = 'iot-traffic-congestion'
CSV_FILE_PATH = 'traffic_data.csv'

def create_producer():
    try:
        producer = KafkaProducer(**PRODUCER_CONF)
        print(f"✅ Đã kết nối Kafka: {PRODUCER_CONF['bootstrap_servers']}")
        return producer
    except Exception as e:
        print(f"❌ Lỗi kết nối Kafka: {e}")
        exit(1)

def load_and_sort_data():
    if not os.path.exists(CSV_FILE_PATH):
        print(f"❌ Không tìm thấy file: {CSV_FILE_PATH}")
        return []

    data = []
    print("⏳ Đang đọc và xử lý dữ liệu...")
    with open(CSV_FILE_PATH, mode='r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            data.append(row)
    
    # Sắp xếp theo timestamp cũ để đảm bảo logic luồng dữ liệu hợp lý
    data.sort(key=lambda x: datetime.strptime(x['timestamp'], '%d/%m/%Y %H:%M'))
    print(f"✅ Đã tải {len(data)} dòng dữ liệu.")
    return data

def stream_data():
    producer = create_producer()
    all_data = load_and_sort_data()

    if not all_data:
        return

    print("🚀 Bắt đầu streaming (Timestamp = NOW)...")
    
    try:
        while True:
            # Gom nhóm theo timestamp CSV để lấy đúng bộ 5 sensor cùng lúc
            for _, group in groupby(all_data, key=lambda x: x['timestamp']):
                
                # --- THAY ĐỔI: LẤY GIỜ HIỆN TẠI ---
                # Định dạng: ngày/tháng/năm Giờ:Phút:Giây (Thêm giây để chi tiết hơn)
                current_timestamp = datetime.now().strftime('%d/%m/%Y %H:%M:%S')
                
                print(f"\n⏰ Sending Batch at: {current_timestamp}")
                
                rows_in_batch = list(group) # Chuyển group thành list để duyệt

                for row in rows_in_batch:
                    try:
                        sensor_id = f"LOC_{row['location_id']}"
                        
                        message = {
                            "sensor_id": sensor_id,
                            # Thay thế timestamp cũ bằng timestamp hiện tại
                            "timestamp": current_timestamp, 
                            
                            # --- FEATURE COLUMNS ---
                            "traffic_volume": int(row['traffic_volume']),
                            "avg_vehicle_speed": float(row['avg_vehicle_speed']),
                            "vehicle_breakdown": {
                                "cars": int(row['vehicle_count_cars']),
                                "trucks": int(row['vehicle_count_trucks']),
                                "bikes": int(row['vehicle_count_bikes'])
                            },
                            
                            "weather": {
                                "condition": row['weather_condition'],
                                "temperature": float(row['temperature']),
                                "humidity": float(row['humidity'])
                            },
                            "road_status": {
                                "accident_reported": bool(int(row['accident_reported'])),
                                "signal_status": row['signal_status']
                            }
                        }

                        producer.send(
                            topic=TOPIC_NAME, 
                            key=sensor_id, 
                            value=message
                        )
                        
                        print(f"   >> Gửi Key={sensor_id} | Time={message['timestamp']} | Vol={message['traffic_volume']}")

                    except Exception as e:
                        print(f"⚠️ Lỗi dòng dữ liệu: {e}")

                producer.flush()
                
                # Chờ 5s
                print("💤 Chờ 5s...")
                time.sleep(5)

            print("\n🔄 Đã phát hết file CSV, quay lại từ đầu (Vẫn dùng giờ hiện tại)...")
            time.sleep(2)

    except KeyboardInterrupt:
        print("\n🛑 Dừng streaming.")
    finally:
        producer.close()

if __name__ == "__main__":
    stream_data()