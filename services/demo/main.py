# from fastapi import FastAPI
# import json
# import time
# import logging

# app = FastAPI()

# # We'll connect to Kafka lazily when needed
# def get_kafka_producer():
#     from kafka import KafkaProducer
#     try:
#         producer = KafkaProducer(
#             bootstrap_servers=['kafka:29092'],
#             value_serializer=lambda v: json.dumps(v).encode('utf-8')
#         )
#         return producer
#     except Exception as e:
#         logging.error(f"Failed to connect to Kafka: {e}")
#         return None

# @app.get("/")
# def read_root():
#     return {"message": "Hello from demo service!"}

# @app.post("/publish/{msg}")
# def publish_message(msg: str):
#     producer = get_kafka_producer()
#     if producer is None:
#         return {"error": "Cannot connect to Kafka"}
    
#     try:
#         producer.send('test-topic', {'message': msg})
#         producer.flush()
#         return {"status": "success", "message": msg}
#     except Exception as e:
#         return {"error": str(e)}

# @app.get("/publish/{msg}")
# def publish_message_get(msg: str):
#     return publish_message(msg)


from fastapi import FastAPI
from kafka import KafkaProducer
import json, time, logging, threading, random, uuid, datetime

app = FastAPI()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

producer = KafkaProducer(
    bootstrap_servers=['kafka:29092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def generate_event():
    """Generate a random transaction-like event."""
    return {
        "transaction_id": str(uuid.uuid4()),
        "user_id": f"user-{random.randint(1000, 9999)}",
        "amount": round(random.uniform(5.0, 500.0), 2),
        "currency": random.choice(["USD", "EUR", "GBP"]),
        "merchant": random.choice(["Amazon", "Ebay", "Walmart", "Apple"]),
        "location": random.choice(["US-NY", "US-CA", "DE-BE", "UK-LON"]),
        "timestamp": datetime.datetime.utcnow().isoformat(),
        "status": random.choice(["PENDING", "COMPLETE", "FAILED"]),
        "payment_method": random.choice(["CREDIT_CARD", "PAYPAL", "APPLE_PAY"]),
        "category": random.choice(["ELECTRONICS", "GROCERY", "FASHION"]),
        "device_id": f"device-{random.randint(1000,9999)}",
        "ip_address": f"192.168.{random.randint(0,255)}.{random.randint(0,255)}",
        "lat": round(random.uniform(-90.0, 90.0), 4),
        "lon": round(random.uniform(-180.0, 180.0), 4),
        "channel": random.choice(["WEB", "MOBILE_APP", "POS"])
    }

def background_producer():
    while True:
        event = generate_event()
        producer.send("transactions", event)
        logger.info(f"Produced event: {event}")
        time.sleep(3)  # send every 3 seconds

@app.on_event("startup")
def startup_event():
    thread = threading.Thread(target=background_producer, daemon=True)
    thread.start()

@app.get("/")
def read_root():
    return {"message": "Demo service producing transaction events"}


