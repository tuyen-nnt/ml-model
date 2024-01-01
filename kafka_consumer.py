# Consume Kafka to predict label
import json
from kafka import KafkaConsumer, TopicPartition, KafkaProducer
import pandas as pd

def consume_kafka(model, request_frequencies, label_encoder):
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
                original_input_message = json.loads(input_message)["request"]
                print(input_message)

                # Convert message to  pandas series
                input_message_series = pd.Series(name='request', data=[original_input_message])

                print("input_message_series before map")
                print(input_message_series)
                input_message_freq = input_message_series.map(request_frequencies)
                print("input_message_freq after map")
                print(input_message_freq)

                # If the pandas series with frequency has N/A value, then set it "No" and map again
                if input_message_freq.isna().any():
                    input_message = "No"
                    # Process each message
                    # Convert message to  pandas series
                    input_message_series = pd.Series(name='request', data=[input_message])
                    input_message_freq = input_message_series.map(request_frequencies)

                input_message_freq = pd.concat([input_message_series, input_message_freq.rename('request_frequency')], axis=1)

                # Drop the original 'request' column
                input_message_freq = input_message_freq.drop(labels='request', axis=1)
                print("input_message_freq")
                print(input_message_freq)

                # Convert to a matrix that is usable for sklearn prediction
                input_message_matrix = pd.DataFrame(input_message_freq)
                print(input_message_matrix)


                # Transform the message (you can replace this with your transformation logic)
                predicted_message = model.predict(input_message_matrix)
                print(predicted_message)

                # Use label encoder to inverse back to value 200 or 404 instead of 0,1
                predicted_message_label_decoded = str(label_encoder.inverse_transform(predicted_message)[0])
                print(predicted_message_label_decoded)

                # Construct a dictionary with the "response" key
                response_dict = {"request": original_input_message, "response": predicted_message_label_decoded}

                # Convert the dictionary to a JSON string
                json_string = json.dumps(response_dict)

                # Print the JSON string
                print("JSON String:", json_string)
                # Produce the transformed message to another topic
                producer.send('result', value=json_string.encode('utf-8'))
                print("Sent the predicted result to Kafka! DONE")

    # Close the consumer and producer when done
    consumer.close()
    producer.close()


# Consume message: root@jupyter:~/kafka_2.13-3.6.1/bin# ./kafka-console-consumer.sh --topic quickstart-events --from-beginning --bootstrap-server 172.31.70.3:9092
# Produce bin/kafka-console-producer.sh --topic predicted --bootstrap-server 172.31.70.3:9092
# {"request" : "/opensearch/opensearch-1.0.0.xxx", "response" : 404}
# {"request" : "/beats/metricbeat/metricbeat-6.3.2-amd64.deb", "response" : 404}


