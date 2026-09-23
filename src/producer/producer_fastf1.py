import json
import time
import argparse
import requests
from kafka import KafkaProducer

def get_kafka_producer(broker_address: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=[broker_address],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: str(k).encode('utf-8'),
        retries=3,
        acks='all'
    )
    
def fetch_endpoint(endpoint: str, session_key: int, driver_number: int):
    url = f"https://api.openf1.org/v1/{endpoint}?session_key={session_key}&driver_number={driver_number}"
    print(f"Fetching {endpoint} data from {url} ...")

    try:
        response = requests.get(url, timeout=10)
    except requests.exceptions.RequestException as e:
        print(f"Network error fetching {endpoint}: {e}")
        return []

    if response.status_code != 200:
        print(f"{endpoint} returned HTTP {response.status_code}, skipping.")
        return []

    data = response.json()
    print(f"Fetched {len(data)} packets for {endpoint}.")
    return data

def stream_pipeline(
    session_key: int = 9165,
    driver_number: int = 22,
    broker: str = 'localhost:9092',
    delay: float = 0.05
):
    producer = get_kafka_producer(broker)
    
    car_data = fetch_endpoint('car_data', session_key, driver_number)
    location_data = fetch_endpoint('location', session_key, driver_number)
    interval_data = fetch_endpoint('intervals', session_key, driver_number)
    
    print("Merging streams into chronological order...")

    combined = []

    for record in car_data:
        combined.append(('f1-telemetry-raw', record))

    for record in location_data:
        combined.append(('f1-location-raw', record))

    for record in interval_data:
        combined.append(('f1-intervals-raw', record))

    combined.sort(key=lambda item: item[1]['date'])

    print(f"Streaming {len(combined)} packets to dedicated Kafka topics...")

    for topic, record in combined:
        producer.send(topic, key=record.get('driver_number'), value=record)

        if delay > 0:
            time.sleep(delay)

    producer.flush()
    print(f"Finished multi-topic streaming for car #{driver_number}.")
    
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Multi-Topic OpenF1 API Produce")
    parser.add_argument('--session_key', type=int, default=9165)
    parser.add_argument('--driver', type=int, default=22)
    parser.add_argument('--broker', type=str, default='localhost:9092')
    parser.add_argument('--delay', type=float, default=0.01)
            
    args = parser.parse_args()

    stream_pipeline(
        session_key=args.session_key,
        driver_number=args.driver,
        broker=args.broker,
        delay=args.delay
    )
    
    
    
    
    
    
    


