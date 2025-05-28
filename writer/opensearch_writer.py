import os
from opensearchpy import OpenSearch
from opensearchpy.exceptions import OpenSearchException
from opensearchpy.helpers import bulk


class OpenSearchWriter:

    def __init__(self, config: dict):
        self.index = config["index"]
        self.host = config["host"]
        self.use_ssl = config.get("use_ssl", False)
        self.verify_certs = config.get("verify_certs", False)

        self.username = os.getenv("OPENSEARCH_USERNAME")
        self.password = os.getenv("OPENSEARCH_PASSWORD")
        if not self.username or not self.password:
            raise EnvironmentError("OpenSearch credentials not provided")

        self.client = OpenSearch(
            hosts=[self.host],
            http_auth=(self.username, self.password),
            use_ssl=self.use_ssl,
            verify_certs=self.verify_certs,
        )

        if not self.client.ping():
            raise ConnectionError(f"Failed to connect to OpenSearch host: {self.host}")

    def write_documents(self, doc, doc_id=None):
        try:
            response = self.client.index(index=self.index, id=doc_id, body=doc)
            return response
        except OpenSearchException as e:
            raise RuntimeError(f"Failed to write document to index '{self.index}': {e}")

    def bulk_write_documents(self, documents):
        try:
            actions = [{"_index": self.index, "_source": doc} for doc in documents]
            success, _ = bulk(self.client, actions)
            return success
        except OpenSearchException as e:
            raise RuntimeError(f"Failed to perform bulk write to OpenSearch: {e}")
