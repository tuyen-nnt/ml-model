from esExport import retrieve_all_records
from train import trainModel
from kafka_consumer import  consume_kafka

if __name__ == "__main__":
    print("Connecting to elasticsearch")
    df = retrieve_all_records()
    model = trainModel(df)
    consume_kafka(model)