from esExport import retrieve_all_records
from train import trainModel

if __name__ == "__main__":
    print("Connecting to elasticsearch")
    df = retrieve_all_records()
    model = trainModel(df)