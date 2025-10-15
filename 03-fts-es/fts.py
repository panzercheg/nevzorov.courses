from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import time
import os

default_host = "localhost" if os.getenv("CODESPACES") == "true" else "127.0.0.1"
ES_HOST = os.getenv("ES_HOST", default_host)

# Add waiting

def wait_for_elasticsearch(max_retries=10, delay=10):
    for attempt in range(max_retries):
        try:
            es = Elasticsearch(
                f"http://{ES_HOST}:9200",
                headers={
                    "Accept": "application/vnd.elasticsearch+json; compatible-with=8",
                    "Content-Type": "application/vnd.elasticsearch+json; compatible-with=8"
                },
                request_timeout=5
            )
            if es.ping():
                return es
        except ConnectionError:
            pass

        print(f"Ожидание Elasticsearch... попытка {attempt + 1}/{max_retries}")
        if attempt < max_retries - 1:
            time.sleep(delay)

es = wait_for_elasticsearch(max_retries=10, delay=10)
INDEX = "test_texts"

# Delete if exists
if es.indices.exists(index=INDEX):
    es.indices.delete(index=INDEX)

# Create with synonym filter
es.indices.create(
    index=INDEX,
    body={
        "settings": {
            "analysis": {
                "filter": {
                    "cat_synonyms": {
                        "type": "synonym",
                        "synonyms": [
                            "cat, kitten",
                            "wine, water"
                        ]
                    },
                    "english_stemmer": {
                        "type": "stemmer",
                        "language": "english"
                    }
                },
                "analyzer": {
                    "cat_english": {
                        "tokenizer": "standard",
                        "filter": [
                            "lowercase",
                            "cat_synonyms",
                            "english_stemmer"
                        ]
                    },
                    "default": {
                        "tokenizer": "standard",
                        "filter": [
                            "lowercase",
                            "cat_synonyms",
                            "english_stemmer"
                        ]
                    }
                }
            }
        },
        "mappings": {
            "properties": {
                "text": {"type": "text", "analyzer": "cat_english"}
            }
        }
    }
)

docs = [
    {"_index": INDEX, "_id": 1, "text": "The cat sat on the mat."},
    {"_index": INDEX, "_id": 2, "text": "A kitten is playing with yarn."},
    {"_index": INDEX, "_id": 3, "text": "Cats are wonderful pets."},
    {"_index": INDEX, "_id": 4, "text": "He adopted a small kitten."},
    {"_index": INDEX, "_id": 5, "text": "The dog chased the cat."},
    {"_index": INDEX, "_id": 6, "text": "She loves her cats and kittens."},
    {"_index": INDEX, "_id": 7, "text": "The feline was sleeping."},
    {"_index": INDEX, "_id": 8, "text": "Beer."},
    {"_index": INDEX, "_id": 9, "text": "Vodka."},
    {"_index": INDEX, "_id": 10, "text": "Whiskey."},
    {"_index": INDEX, "_id": 11, "text": "Wine."},
    {"_index": INDEX, "_id": 12, "text": "Water."},
]
bulk(es, docs)
time.sleep(1)

def search_and_print(query):
    print(f"\nSearching for '{query}'...")
    res = es.search(
        index=INDEX,
        body={
            "query": {
                "match": {"text": query}
            }
        }
    )
    for hit in res['hits']['hits']:
        print(f"Score: {hit['_score']:.2f} | Text: {hit['_source']['text']}")

# Remove previous direct search/print blocks
search_and_print("cat")
search_and_print("kitten")
search_and_print("wine") 