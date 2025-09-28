from kafka import KafkaConsumer
import json
import time
import logging
import psycopg2

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def save_to_db(message):
    try:
        conn = psycopg2.connect(
            dbname="events",
            user="demo",
            password="demo",
            host="postgres"
        )
        cur = conn.cursor()
        cur.execute("CREATE TABLE IF NOT EXISTS messages (id SERIAL PRIMARY KEY, content JSONB)")
        cur.execute("INSERT INTO messages (content) VALUES (%s)", (json.dumps(message),))
        conn.commit()
        cur.close()
        conn.close()
        logger.info("Message saved to Postgres")
    except Exception as e:
        logger.error(f"DB error: {e}")

def main():
    # Wait a bit for Kafka to be ready
    time.sleep(10)
    
    logger.info("Starting Kafka consumer...")
    
    try:
        consumer = KafkaConsumer(
            'test-topic',
            bootstrap_servers=['kafka:29092'],
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='demo-consumer-group',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        
        logger.info("Connected to Kafka, listening for messages...")
        
        for message in consumer:
            logger.info(f"Received message: {message.value}")
            logger.info(f"Topic: {message.topic}, Partition: {message.partition}, Offset: {message.offset}")
            save_to_db(message.value)

    except Exception as e:
        logger.error(f"Error in consumer: {e}")
        time.sleep(5)
        main()  # Retry

if __name__ == "__main__":
    main()
