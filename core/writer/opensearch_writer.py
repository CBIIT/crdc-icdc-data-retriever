import json
import logging
import os

from opensearchpy import OpenSearch
from opensearchpy.exceptions import OpenSearchException
from opensearchpy.helpers import bulk

logger = logging.getLogger(__name__)


class OpenSearchWriter:

    def __init__(self, config: dict):
        output_config = config.get("output", {}).get("config", {})
        self.index = output_config["index"]
        self.host = output_config["host"]
        self.use_ssl = output_config.get("use_ssl", False)
        self.verify_certs = output_config.get("verify_certs", False)

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
            logger.error(f"Failed to connect to OpenSearch host: {self.host}")
            raise ConnectionError(f"Failed to connect to OpenSearch host: {self.host}")

        logger.info(f"Connected to OpenSearch host: {self.host}")

    def write_documents(self, doc, doc_id=None):
        try:
            response = self.client.index(index=self.index, id=doc_id, body=doc)
            if response.get("result") in {"created", "updated"}:
                logger.info(
                    f"Document {response.get('_id')} successfully {response['result']} in index '{self.index}'"
                )
            else:
                logger.warning(
                    f"Unexpected indexing result for document {response.get('_id')}: {response.get('result')}"
                )
            return response
        except OpenSearchException as e:
            logger.error(f"Failed to write documents to index {self.index}: {e}")
            raise RuntimeError(f"Failed to write document to index '{self.index}': {e}")

    def bulk_write_documents(self, documents):
        try:
            documents = OpenSearchWriter._ensure_json_serializable(documents)
            actions = [{"_index": self.index, "_source": doc} for doc in documents]
            success, _ = bulk(self.client, actions)
            logger.info(
                f"Wrote {success} out of {len(documents)} documents to index {self.index}"
            )
            return success
        except OpenSearchException as e:
            logger.error(f"Failed to write documents to index {self.index}: {e}")
            raise RuntimeError(f"Failed to perform bulk write to OpenSearch: {e}")

    @staticmethod
    def _ensure_json_serializable(documents):
        serializable_docs = []
        for doc in documents:
            try:
                json.dumps(doc)
                serializable_docs.append(doc)
            except (TypeError, ValueError) as e:
                logger.warning(f"Skipping unserializable document: {e}")
        return serializable_docs
