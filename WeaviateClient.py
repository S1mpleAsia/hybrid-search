import weaviate
from weaviate.util import generate_uuid5


def construct_where_filter(genres):
    if not genres:
        return {
            "path": ["title"],
            "operator": "Like",
            "valueText": "*"
        }
    return {
        "path": ["genre_names"],
        "operator": "ContainsAll",
        "valueStringArray": genres
    }


class WeaviateClient:
    def __init__(self, url, api_key=None):
        self.client = weaviate.Client(
            url=url,
            auth_client_secret=weaviate.auth.AuthApiKey(api_key=api_key),
            additional_headers={
                "X-Huggingface-Api-Key": "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"
            }
        )

    def create_schema(self, class_name, properties):
        class_obj = {
            # Class & property definitions
            "class": class_name,
            "properties": [
                {
                    "name": prop,
                    "dataType": [data_type]
                } for prop, data_type in properties.items()
            ],

            # Specify a vectorizer
            "vectorizer": "text2vec-huggingface",

            # Module settings
            "moduleConfig": {
                "text2vec-huggingface": {
                    "vectorizeClassName": False,
                    "model": "sentence-transformers/all-MiniLM-L6-v2",
                    "options": {
                        "waitForModel": 'true',
                        "useGPU": 'true',
                        "useCache": 'true'
                    }
                }
            },
        }

        try:
            self.client.schema.create_class(class_obj)
            print(f"Schema for class '{class_name} created successfully.")
        except Exception as e:
            print(f"Failed to query objects: {e}")
            return None

    def import_data(self, data, class_name):
        with self.client.batch(
                batch_size=200,
                num_workers=2,
        ) as batch:
            for i, row in enumerate(data):
                movie_object = {
                    "movie_id": row["movie_id"],
                    "title": row["title"],
                    "corpus": row["corpus"],
                    "genre_names": row["genre_names"],
                    "rating": row["rating"],
                    "popularity": row["popularity"],
                    "released_date": row["released_date"]
                }

                batch.add_data_object(
                    movie_object,
                    class_name=class_name,
                    uuid=generate_uuid5(movie_object)
                )

        assert \
            self.client.query.aggregate(class_name).with_meta_count().do()["data"]["Aggregate"][class_name][0]["meta"][
                "count"] != 0

    def suggest(self, query, limit):
        response = self.client.query \
            .get("Movie", ["movie_id", "title", "genre_names", "corpus"]) \
            .with_hybrid(
                query=query,
                properties=["corpus", "genre_names"]
            ) \
                .with_additional(["score", "explainScore"]) \
                .with_limit(limit) \
                .do()

        return response

    def search(self, query, limit, genres):

        response = self.client.query \
            .get("Movie", ["movie_id", "title", "genre_names", "corpus", "rating", "popularity", "released_date"]) \
            .with_hybrid(
                query=query,
                properties=["corpus", "genre_names"]
            ) \
                .with_additional(["score", "explainScore"]) \
                .with_limit(limit) \
                .with_where(construct_where_filter(genres=genres)) \
                .do()

        print(response)

        return response
