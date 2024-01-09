import requests
import pandas as pd

def get_total_record_count():
    base_url = "https://172.31.70.3:9200"
    index_name = "opensearch_dashboards_sample_data_logs"
    auth = ("admin", "admin")  # Replace with your Elasticsearch username and passwordv

    count_url = f"{base_url}/{index_name}/_count"
    print(count_url)
    query = {
        "query": {
            # Your query goes here
            "match_all": {}  # Example: Match all documents
        }
    }

    response = requests.get(count_url, auth=auth, verify=False)
    print("Connected to elasticsearch")
    total_records = response.json().get("count", 0)
    print(total_records)
    return total_records

def get_elasticsearch_data(scroll_id=None):
    base_url = "https://172.31.70.3:9200"
    index_name = "opensearch_dashboards_sample_data_logs"
    scroll_timeout = "10m"  # 10 minutes
    auth = ("admin", "admin")  # Replace with your Elasticsearch username and password

    if scroll_id is None:
        # Initial search request
        search_url = f"{base_url}/{index_name}/_search?scroll={scroll_timeout}"
        query = {
            # "size": 10,  # Number of documents per scroll
            "query": {
                # Your query goes here
                "match_all": {}  # Example: Match all documents
            }
        }
        response = requests.post(search_url, json=query, auth=auth, verify=False)
    else:
        # Subsequent scroll requests
        scroll_url = f"{base_url}/_search/scroll"
        scroll_data = {"scroll": scroll_timeout, "scroll_id": scroll_id}
        response = requests.post(scroll_url, json=scroll_data, auth=auth, verify=False)

    return response.json()

def retrieve_all_records():
    total_records = get_total_record_count()
    records_per_scroll = 1000
    scroll_id = None
    df = pd.DataFrame([])


    while total_records > 0:
        response_data = get_elasticsearch_data(scroll_id)
        hits = response_data.get("hits", {}).get("hits", [])

        if not hits:
            break  # No more results & terminate the loop immediately

        # Initialize the DataFrame with the first batch of data
        df = df.append([hit['_source'] for hit in hits])

        total_records -= records_per_scroll
        scroll_id = response_data.get("_scroll_id")

    # Create a dataframe.
    df = pd.DataFrame(temp)
    return df
    # Clear the scroll context when done
    # clear_scroll_url = "http://172.31.70.3:9200/_search/scroll"
    # requests.delete(clear_scroll_url, json={"scroll_id": [scroll_id]})

