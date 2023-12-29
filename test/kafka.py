import json

from kafka import KafkaConsumer, KafkaProducer, TopicPartition

topic_name = "quickstart-events"

consumer = KafkaConsumer(
     bootstrap_servers='172.31.70.3:9092',
     auto_offset_reset='latest',
     group_id='jupyter'
     # enable_auto_commit=True,
)

# Initialize KafkaProducer with your configuration for producing
producer = KafkaProducer(
    bootstrap_servers='172.31.70.3:9092'
)

print("Connected to Kafka")

# Assign partitions manually to control the offset
topic_partitions = consumer.partitions_for_topic(topic_name)

# Get list partition
partitions_to_assign = [TopicPartition(topic_name, partition) for partition in topic_partitions]
consumer.assign(partitions_to_assign)
# seek to beginning
consumer.seek_to_beginning()

# Set the batch size
batch_size = 1000

while True:
    # Use the poll method to fetch messages
    messages = consumer.poll(timeout_ms=1000, max_records=batch_size)
    if not messages:
        # No new messages, continue with the next iteration
        # print('No new messages')
        continue

    for tp, records in messages.items():
        for record in records:
            # Process each message
            print(f"Received message: {record.value.decode('utf-8')}")
            # Transform the message (you can replace this with your transformation logic)
            # Process each message
            input_message = record.value.decode('utf-8')
            input_message = json.loads(input_message)["request"]
            print(input_message)
            # Transform the message (you can replace this with your transformation logic)
            # predicted_message = model.predict(input_message)

            # Produce the transformed message to another topic
            # producer.send('predicted', value=predicted_message.encode('utf-8'))