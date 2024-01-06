from esExport import retrieve_all_records
from train import trainModel
from kafka_consumer import  consume_kafka
import logging

if __name__ == "__main__":

    logging.basicConfig(filename="/dev/stdout",
                                filemode='a',
                                format='[%(asctime)s,%(msecs)d] [%(filename)s:%(lineno)d] [%(levelname)s] %(message)s',
                                datefmt='%Y-%m-%d %H:%M:%S',
                                level=logging.INFO
                                )
    logging.info("Connecting to elasticsearch")
    df = retrieve_all_records()
    frequencies, model, label_encoder = trainModel(df, logging)
    consume_kafka(model, frequencies, label_encoder, logging)