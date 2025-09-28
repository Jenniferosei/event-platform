from fastapi import FastAPI
import json
import time
import logging

app = FastAPI()

# We'll connect to Kafka lazily when needed
def get_kafka_producer():
    from kafka import KafkaProducer
    try:
        producer = KafkaProducer(
            bootstrap_servers=['kafka:29092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        return producer
    except Exception as e:
        logging.error(f"Failed to connect to Kafka: {e}")
        return None

@app.get("/")
def read_root():
    return {"message": "Hello from demo service!"}

@app.post("/publish/{msg}")
def publish_message(msg: str):
    producer = get_kafka_producer()
    if producer is None:
        return {"error": "Cannot connect to Kafka"}
    
    try:
        producer.send('test-topic', {'message': msg})
        producer.flush()
        return {"status": "success", "message": msg}
    except Exception as e:
        return {"error": str(e)}

@app.get("/publish/{msg}")
def publish_message_get(msg: str):
    return publish_message(msg)

