from kafka import KafkaConsumer
import schedule
import time

def poll_kafka():
    # Use the poll method to fetch messages
    messages = consumer.poll(timeout_ms=100, max_records=batch_size)

    if not messages:
        # No new messages
        print("No new messages.")
        return

    for tp, records in messages.items():
        for record in records:
            # Process each message
            print(f"Received message: {record.value.decode('utf-8')}")

# Initialize KafkaConsumer with your configuration
consumer = KafkaConsumer(
    'your_topic_name',
    bootstrap_servers=['your_bootstrap_servers'],
    group_id='your_group_id'
)

# Set the batch size
batch_size = 1000

# Schedule the poll_kafka function to run twice a day
schedule.every().day.at("08:00").do(poll_kafka)
schedule.every().day.at("20:00").do(poll_kafka)

# Run the schedule in an infinite loop
while True:
    schedule.run_pending()
    time.sleep(1)  # Sleep to avoid high CPU usage during the wait
