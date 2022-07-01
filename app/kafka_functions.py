#This file contains ad-hoc kafka functions to handle either the producer and the consumer, as well as the admin connection to see logs

import json
from typing import Any, Dict

from kafka import KafkaConsumer, KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic

def create_kafka_topic(topicName: str) -> None:
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers="localhost:9091", 
            client_id='cl1'
        )
        topic_list = []
        topic_list.append(NewTopic(name=topicName, num_partitions=1, replication_factor=1))
        admin_client.create_topics(new_topics=topic_list, validate_only=False)
        print(f'Topic {topicName} created')
    except Exception as e:
        print(f'topic {topicName} not created because of: {e}')

def create_kafka_producer() -> KafkaProducer:
    producer = None
    try:
        producer = KafkaProducer(
            bootstrap_servers=['localhost:9091'],
            api_version=(0, 10),
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        )
    except Exception as e:
        print('Exception while creating producer in Kafka')
        print(str(e))
    finally:
        return producer

def create_kafka_consumer(topic:str) -> KafkaConsumer:
    consumer = None
    try:
        consumer = KafkaConsumer(topic,
            api_version=(0, 11),
            bootstrap_servers=['localhost:9091'],
            auto_offset_reset='latest',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            enable_auto_commit=True
        )
    except Exception as e:
        print('Exception while creating consumer in Kafka')
        print(str(e))
    finally:
        return consumer

def kafka_publish(topic:str, value:Dict, producer:KafkaProducer) -> None:
    try:
        producer.send(topic, value=value)
    except Exception as e:
        print(f"Exception occurred while trying to publish to {topic}")
        print(str(e))
    #print(f'Message published on {topic}')

def create_kafka_connection() -> None:
    create_kafka_topic("vehicles")
    create_kafka_topic('trafficlights')
    create_kafka_topic('edges')
    create_kafka_topic('output')

