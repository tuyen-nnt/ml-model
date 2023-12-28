# Consume Kafka to predict label
from kafka import KafkaConsumer, TopicPartition, KafkaProducer

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

print("Connected")

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

            # Transform the message (you can replace this with your transformation logic)
            # transformed_message = transform_function(input_message)

            # Produce the transformed message to another topic
            producer.send('predicted', value=input_message.encode('utf-8'))

# Close the consumer and producer when done
consumer.close()
producer.close()

# Consume message: root@jupyter:~/kafka_2.13-3.6.1/bin# ./kafka-console-consumer.sh --topic quickstart-events --from-beginning --bootstrap-server 172.31.70.3:9092
# Produce bin/kafka-console-producer.sh --topic predicted --bootstrap-server 172.31.70.3:9092
# {"request" : "/opensearch/opensearch-1.0.0.xxx", "response" : 404}