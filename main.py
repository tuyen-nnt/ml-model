from esExport import retrieve_all_records
from train import trainModel
from kafka_consumer import  consume_kafka

if __name__ == "__main__":
    print("Connecting to elasticsearch")
    df = retrieve_all_records()
    frequencies, model, label_encoder = trainModel(df)
    consume_kafka(model, frequencies, label_encoder)