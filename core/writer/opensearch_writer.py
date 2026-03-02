import hashlib
import json
import logging
import os

from core.processor.post_processor_registry import (
    get_post_processor,
    apply_post_processor,
)

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
            ConnectionError: If client cannot connect to OpenSearch host.
        """
        self.config = config
        self.output_config = self.config.get("output", {}).get("config", {})

        self.index = self.output_config["index"]
        hosts = self.output_config.get("hosts") or self.output_config.get("host")

        if isinstance(hosts, str):
            self.hosts = [hosts] if hosts else []
        elif isinstance(hosts, list):
            self.hosts = [host for host in hosts if host]
        else:
            self.hosts = []

        self.use_ssl = self.output_config.get("use_ssl", False)
        self.verify_certs = self.output_config.get("verify_certs", False)

        self.post_processor = get_post_processor(
            self.output_config.get("post_processor")
        )
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
                serializable_docs = OpenSearchWriter._ensure_json_serializable(
                    documents
                )
                project = self.config.get("project")
                actions = []

                if not serializable_docs:
                    logger.warning("No valid documents to index.")
                    return {"success": 0, "attempted": 0}

                # flatten list of documents in case any fetchers returned lists
                # use generator if documents can be large
                flat_docs = []
                for doc in serializable_docs:
                    if isinstance(doc, list):
                        flat_docs.extend(doc)
                    else:
                        flat_docs.append(doc)

                valid_docs = [doc for doc in flat_docs if doc]

                # defensive check for empty individual fetch results
                if not valid_docs:
                    logger.warning("No valid documents to index.")
                    return {"success": 0, "attempted": 0}

                if self.post_processor:
                    valid_docs = apply_post_processor(self.post_processor, valid_docs)

                for doc in valid_docs:
                    doc_id = OpenSearchWriter._build_doc_id(doc, project)
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

    @staticmethod
    def _build_doc_id(doc: dict, project: str) -> str:
        """
        Builds a document ID for indexing.

        Args:
            doc (dict): The document to index.
            project (str): The project name.

        Returns:
            str: The constructed document ID.
        """
        # handle ICDC-style data
        if "clinical_study_designation" in doc:
            return f"{project}_{doc['clinical_study_designation']}"

        # alternate ICDC format
        if "entity_id" in doc:
            return f"{project}_{doc['entity_id']}"

        # handle CCDI-style data
        if "repository" in doc and "data" in doc:
            if doc.get("repository", "") == "TCIA":
                slug = doc.get("data", {}).get("slug")
                coll_id = doc.get("data", {}).get("id")
                key = slug or coll_id or "unknown"
                return f"{project}_{doc['repository']}_{key}"
            elif doc.get("repository", "") == "IDC":
                data = doc.get("data", {})
                collection_id = data.get("collection_id") or "unknown"
                repo = doc.get("repository", "")
                return f"{project}_{repo}_{collection_id}"

        # hash fallback for all other projects/repositories
        doc_hash = hashlib.md5(json.dumps(doc, sort_keys=True).encode()).hexdigest()[
            :12
        ]
        repository = doc.get("repository", "") or "unknown"

        return f"{project}_{repository}_{doc_hash}"
