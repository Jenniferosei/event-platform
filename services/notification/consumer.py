from kafka import KafkaConsumer
import json, logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    consumer = KafkaConsumer(
        'failed_transactions',
        bootstrap_servers=['kafka:29092'],
        auto_offset_reset='earliest',
        group_id='notification-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    
    logger.info("Notification consumer started. Listening for failed transactions...")

    for message in consumer:
        event = message.value
        logger.info(f"ALERT: Failed transaction detected: {event['transaction_id']} | User: {event['user_id']} | Amount: {event['amount_usd'] if 'amount_usd' in event else event['amount']} {event['currency']}")
        # Optionally: send HTTP POST to a Slack/email webhook

if __name__ == "__main__":
    main()
