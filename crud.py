import time
from elasticsearch import Elasticsearch, helpers
import warnings

warnings.filterwarnings("ignore")

mapping = """
{
	"mappings": {
		"properties": {
			"item_name": {
				"type": "keyword"
			},
			"price": {
				"type": "float"
			}
		}
	}
}"""

index_name = "retail_store"

query = {"query": {"bool": {"must": {"term": {"item_name": "apple"}}}}}


def connection():
	url = "http://root:root@localhost:9200"
	es = Elasticsearch(url)
	return es


def create_index():
	es = connection()
	index_name = "retail_store"

	try:
		index_exists = es.indices.exists(index=index_name)

		if not index_exists:
			es.indices.create(index=index_name, body=mapping)
			print("Index created successfully!")

	except Exception as err:
		print("Elasticsearch error:", err)


def create():
	es = connection()

	# add single doc
	try:
		doc = {"item_name": "orange", "price": 200}
		es.index(index=index_name, doc_type="_doc", body=doc)
		print("single doc added!!")
	except Exception as err:
		print("Error in creating a doc ", err)

	# add multiple docs with bulk api
	try:
		docs = [
			{"item_name": "apple", "price": 100},
			{"item_name": "mango", "price": 150},
			{"item_name": "cherry", "price": 200},
			{"item_name": "litchi", "price": 250},
			{"item_name": "chips", "price": 300},
			{"item_name": "cream", "price": 350},
			{"item_name": "plum", "price": 400},
			{"item_name": "cake", "price": 450},
			{"item_name": "biscuit", "price": 500},
			{"item_name": "chocolate", "price": 550},
		]
		helpers.bulk(es, docs, index=index_name, doc_type="_doc")
		print("multiple docs added!!")
	except Exception as err:
		print("Error in creating multiple doc's ", err)


def read():
	es = connection()

	# basic search query
	try:
		results = es.search(index=index_name, body=query)
		print("Search Results ", results)
	except Exception as err:
		print("Search Error ", err)

	range_query = {"query": {"range": {"price": {"gte": 100}}}}

	try:
		results = es.search(index=index_name, body=range_query)
		print("Number of Search Results ", len(results["hits"]["hits"]))
	except Exception as err:
		print("Error in showing number of results ", err)

	# basic search query based on customized size
	try:
		results = es.search(index=index_name, body=range_query, size=1000)
		print(
				"Number of Search Results with Custom Size ", len(results["hits"]["hits"])
		)
	except Exception as err:
		print("Error in showing number of results ", err)

	# search query with the help of helpers.scan()
	try:
		results = helpers.scan(client=es, query=range_query, index=index_name)
		count = 0
		for result in results:
			count = count + 1
		print("Number of Search Results with Helpers Scan ", count)
	except Exception as err:
		print("Error in showing number of results ", err)


def update():
	es = connection()
	doc_id = ""
	doc_body = {}

	# updation with search
	try:
		doc = es.search(index=index_name, body=query)
		doc_body = doc["hits"]["hits"][0]["_source"]
		doc_id = doc["hits"]["hits"][0]["_id"]

		doc_body["price"] = 50
		es.update(index=index_name, id=doc_id, body={"doc": doc_body})
	except Exception as err:
		print("Error in updating with search: ", err)

	# updation with helpers.scan()
	try:
		doc_results = helpers.scan(client=es, query=query, index=index_name)

		for doc in doc_results:
			doc_body = doc["_source"]
			doc_id = doc["_id"]

		doc_body["price"] = 150
		es.update(index=index_name, id=doc_id, body={"doc": doc_body})
	except Exception as err:
		print("Error in updating with scan: ", err)

def delete():
	es = connection()

	query = {"query": {"range": {"price": {"gte": 400}}}}

	# deletion by query
	try:
		es.delete_by_query(index=index_name, body=query)
		print("Document deleted successfully based on the query!")
	except Exception as err:
		print("Error in deleting: ", err)

if __name__ == "__main__":
	create_index()
	create()
	time.sleep(1)
	read()
	update()
	time.sleep(1)
	delete()
	time.sleep(1)
	read()
