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
        Initialize OpenSearchWriter and connect to host(s) using data provided in the
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
        self.hosts = self.output_config.get("hosts") or self.output_config.get("host")
        self.use_ssl = self.output_config.get("use_ssl", False)
        self.verify_certs = self.output_config.get("verify_certs", False)

        self.username = os.getenv("OPENSEARCH_USERNAME") or self.output_config.get(
            "username"
        )
        self.password = os.getenv("OPENSEARCH_PASSWORD") or self.output_config.get(
            "password"
        )
        self.auth = (
            (self.username, self.password) if self.username and self.password else None
        )
        if not self.auth:
            logger.info(
                "OpenSearch credentials not provided: Attempting connection without authentication."
            )

        self.clients = []
        if isinstance(self.hosts, list):
            for host in self.hosts:
                client = self._make_client(host)
                if not client.ping():
                    logger.error(f"Failed to connect to OpenSearch host: {host}")
                    raise ConnectionError(
                        f"Failed to connect to OpenSearch host: {host}"
                    )
                logger.info(f"Connected to OpenSearch host: {host}")
                self.clients.append(client)

        elif isinstance(self.hosts, str):
            client = self._make_client(self.hosts)
            if not client.ping():
                logger.error(f"Failed to connect to OpenSearch host: {self.hosts}")
                raise ConnectionError(
                    f"Failed to connect to OpenSearch host: {self.hosts}"
                )
            logger.info(f"Connected to OpenSearch host: {self.hosts}")
            self.clients.append(client)

    def _make_client(self, host) -> OpenSearch:
        """
        Create and return an OpenSearch client instance.

        Args:
            host (str): The OpenSearch host to connect to.

        Returns:
            OpenSearch: An instance of the OpenSearch client.
        """
        logger.debug(f"Creating OpenSearch client for host: {host}")

        return OpenSearch(
            hosts=[host],
            http_auth=self.auth,
            use_ssl=self.use_ssl,
            verify_certs=self.verify_certs,
        )

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
        total_attempted = 0
        total_success = 0

        for client in self.clients:
            try:
                documents = OpenSearchWriter._ensure_json_serializable(documents)
                project = self.config.get("project")
                actions = []

                if not documents:
                    logger.warning("No valid documents to index.")
                    return {"success": 0, "attempted": 0}

                # defensive check for empty individual fetch results
                valid_docs = [doc for doc in documents if doc]
                if not valid_docs:
                    logger.warning("No valid documents to index.")
                    return {"success": 0, "attempted": 0}

                for doc in valid_docs:
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

                success, _ = bulk(client, actions)
                total_attempted += len(actions)
                total_success += success
                logger.info(
                    f"Wrote {success} out of {len(documents)} documents to index {self.index} on {client.transport.hosts[0]['host']}"
                )
            except OpenSearchException as e:
                logger.error(
                    f"Write failed on {client.transport.hosts[0]['host']}: {e}",
                    exc_info=True,
                )
                continue

        if total_success == 0:
            raise RuntimeError("Bulk write failed on all configured OpenSearch hosts.")

        return {"success": total_success, "attempted": total_attempted}

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
