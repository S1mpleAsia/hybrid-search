from flask import Flask, request, jsonify
from WeaviateClient import WeaviateClient
import json
import os.path

app = Flask(__name__)

weaviate_url = "https://hybrid-search-4hthz7rh.weaviate.network"
weaviate_api_key = "cOsZkTut0aUUJjZQx4tF9WuzC0Pf8lgtctZt"
weaviate_client = WeaviateClient(weaviate_url, weaviate_api_key)


def read_data(file_name):
    relative_path = os.path.join('data', file_name)

    with open(relative_path, 'r') as f:
        data = json.load(f)
    return data


@app.route("/", methods=['GET'])
def hello_word():
    return jsonify("Hello world")


@app.route("/schema", methods=['POST'])
def create_schema():
    data = request.json
    class_name = data.get('class_name')
    properties = data.get('properties')

    if not class_name or not properties:
        return jsonify({"error": "class_name and object_data are required"}, 400)

    weaviate_client.create_schema(class_name=class_name, properties=properties)
    return jsonify("Create schema successfully.")


@app.route("/data", methods=['POST'])
def import_data():
    data = request.json
    class_name = data.get('class_name')

    data = read_data("movie_df_json.json")
    weaviate_client.import_data(data, class_name=class_name)
    print('import data successfully.')
    return jsonify("import data successfully.")


@app.route("/recommend", methods=['GET'])
def recommend_search():
    query = request.args.get('query', '')
    limit = int(request.args.get('limit', 20))
    response = weaviate_client.suggest(query, limit)
    return jsonify(response["data"]["Get"]["Movie"])


@app.route("/search", methods=['GET'])
def hybrid_search():
    query = request.args.get('query', '')
    limit = int(request.args.get('limit', 20))
    genres = request.args.get('genres', '').split(',') if request.args.get('genres') else []
    print(query, limit, genres)

    response = weaviate_client.search(query, limit, genres)
    return jsonify(response["data"]["Get"]["Movie"])


if __name__ == "__main__":
    app.run(debug=True)
