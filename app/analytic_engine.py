import json
import traci
from kafka import KafkaConsumer, KafkaProducer

import kafka_functions as kf

def analyse() -> None:
    
    return None

def consume_message(consumer: KafkaConsumer, producer: KafkaProducer) -> None:
    for message in consumer:
        print(message.value)


if __name__ == '__main__':
    consumer = kf.create_kafka_consumer('output')
    producer = kf.create_kafka_producer()
    consume_message(consumer, producer)

