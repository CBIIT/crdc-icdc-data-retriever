import json
import logging
import os

from opensearchpy import OpenSearch
from opensearchpy.exceptions import OpenSearchException
from opensearchpy.helpers import bulk

logger = logging.getLogger(__name__)


class OpenSearchWriter:
    """
    Handles connection to OpenSearch host and writing documents to indices.
    """

    def __init__(self, config: dict):
        """
        Initialize OpenSearchWriter and connect to host using data provided in the
        'output' config block.

        Args:
            config (dict): App config data.

        Raises:
            EnvironmentError: If required OpenSearch credentials are missing.
            ConnectionError: If client cannot connect to OpenSearch host.
        """
        self.config = config
        self.output_config = self.config.get("output", {}).get("config", {})
        self.index = self.output_config["index"]
        self.host = self.output_config["host"]
        self.use_ssl = self.output_config.get("use_ssl", False)
        self.verify_certs = self.output_config.get("verify_certs", False)

        self.username = os.getenv("OPENSEARCH_USERNAME") or self.output_config.get(
            "username"
        )
        self.password = os.getenv("OPENSEARCH_PASSWORD") or self.output_config.get(
            "password"
        )

        auth = (
            (self.username, self.password) if self.username and self.password else None
        )
        if not auth:
            logger.warning(
                "OpenSearch credentials not provided: Attempting connection without authentication."
            )

        self.client = OpenSearch(
            hosts=[self.host],
            http_auth=auth,
            use_ssl=self.use_ssl,
            verify_certs=self.verify_certs,
        )

        if not self.client.ping():
            logger.error(f"Failed to connect to OpenSearch host: {self.host}")
            raise ConnectionError(f"Failed to connect to OpenSearch host: {self.host}")

        logger.info(f"Connected to OpenSearch host: {self.host}")

    def bulk_write_documents(self, documents: list) -> dict:
        """
        Bulk write documents to an OpenSearch index.

        Args:
            documents (list): A list of documents containing data.

        Returns:
            dict: Summary of bulk write results (successful / attempted).

        Raises:
            RuntimeError: If bulk write to index fails.
        """
        try:
            documents = OpenSearchWriter._ensure_json_serializable(documents)
            project = self.config.get("project")
            actions = []

            for doc in documents:
                entity_id = doc.get("entity_id")
                source_name = doc.get("CRDCLinks", [{}])[0].get("repository")

                if not entity_id or not source_name:
                    logger.warning(
                        "Skipping document due to missing repository or entity ID."
                    )
                    continue

                doc_id = f"{project}_{source_name}_{entity_id}"
                actions.append(
                    {
                        "_index": self.index,
                        "_id": doc_id,
                        "_source": doc,
                    }
                )

            skipped = len(documents) - len(actions)
            if skipped:
                logger.warning(
                    f"Skipped {skipped} documents due to missing required fields."
                )

            success, _ = bulk(self.client, actions)
            logger.info(
                f"Wrote {success} out of {len(documents)} documents to index {self.index}"
            )
            return {"success": success, "attempted": len(actions)}
        except OpenSearchException as e:
            logger.error(f"Failed to write documents to index {self.index}: {e}")
            raise RuntimeError(f"Failed to perform bulk write to OpenSearch: {e}")

    @staticmethod
    def _ensure_json_serializable(documents: list) -> list:
        """
        Filters out documents that are not JSON-serializable.

        Args:
            documents (list): A list of candidate documents.

        Returns:
            list: List of documents that are JSON-serializable.
        """
        serializable_docs = []
        for doc in documents:
            try:
                json.dumps(doc)
                serializable_docs.append(doc)
            except (TypeError, ValueError) as e:
                logger.warning(f"Skipping unserializable document: {e}")
        return serializable_docs
