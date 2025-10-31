import copy
import logging
import re
from datetime import datetime, timezone
from typing import Callable, Any

from html2text import HTML2Text

from utils.post_processor_utils import deep_merge_additive

logger = logging.getLogger(__name__)


def post_processor(fn: Callable[..., Any]):
    """Labels a function as a post-processor by setting attribute.

    Args:
        fn (Callable[..., Any]): Function to be labeled post-processor.

    Returns:
        Callable[..., Any]: Original function with an '_is_post_processor'
        attribute added.
    """
    fn._is_post_processor = True
    return fn


def transform_html(html: str) -> str:
    """Transforms HTML to plain text.

    Args:
        html (str): HTML string to transform.

    Returns:
        str: Plain-text version of HTML string.
    """
    converter = HTML2Text()
    converter.ignore_links = True
    converter.body_width = 0
    converter.ignore_emphasis = True
    converter.single_line_break = True

    text = converter.handle(html)
    text = re.sub(r"\s+", " ", text).strip()

    return text


@post_processor
def clean_idc_metadata(metadata_list: list[dict]) -> list[dict]:
    """Transforms 'description' fields in IDC metadata from HTML to plain text.

    Args:
        metadata_list (list[dict]): List of IDC metadata dicts.

    Returns:
        list[dict]: Updated metadata with transformed 'description' values.
    """
    for metadata in metadata_list:
        if "description" in metadata:
            metadata["description"] = transform_html(metadata["description"])
            logger.info("Transformed HTML in 'description' field of metadata")
        else:
            logger.warning("'description' key not found in metadata.")
    return metadata_list


@post_processor
def aggregate_tcia_series_data(
    data: list, entity: dict, collection_id: str, entity_id_key: str
) -> dict:
    """Aggregates TCIA metadata fields for a given entity.

    Args:
        data (list[dict]): Array of TCIA metadata dicts.
        entity (dict): Entity record being processed.
        collection_id (str): ID of TCIA data collection.
        entity_id_key (str): Key used to identify entity in project metadata.

    Returns:
        dict: A dict of aggregated metadata fields for the collection.
    """
    ENTITY_OVERRIDES = {
        "GLIOMA01": {
            "Aggregate_ImageCount": 84,
            "Aggregate_Modality": ["Histopathology"],
        }
    }

    total_images = 0
    total_patients = set()
    unique_modalities = set()
    unique_bodyparts = set()

    for item in data:
        total_images += item["ImageCount"]
        total_patients.add(item["PatientID"])
        unique_modalities.add(item["Modality"])
        unique_bodyparts.add(item["BodyPartExamined"])

    result = {
        "Collection": collection_id,
        "Aggregate_PatientID": len(total_patients),
        "Aggregate_Modality": list(unique_modalities),
        "Aggregate_BodyPartExamined": list(unique_bodyparts),
        "Aggregate_ImageCount": total_images,
    }

    entity_id = entity.get(entity_id_key)
    if entity_id in ENTITY_OVERRIDES:
        override = copy.deepcopy(ENTITY_OVERRIDES[entity_id])
        result = deep_merge_additive(result, override)
        logger.info(f"Additional TCIA data for {entity_id} entity added to totals.")

    logger.info(
        f"Completed aggregation of TCIA series data for collection '{collection_id}': "
        f"{result['Aggregate_PatientID']} patients, {result['Aggregate_ImageCount']} images, "
        f"modalities: {sorted(result['Aggregate_Modality'])}, body parts: {sorted(result['Aggregate_BodyPartExamined'])}"
    )

    return result


@post_processor
def format_for_icdc(data: list[dict]) -> list[dict]:
    """Formats fetched and processed data for ICDC ingestion.

    Args:
        data (list[dict]): List of fetched and processed data dicts.

    Returns:
        list[dict]: Formatted data ready for ICDC ingestion.
    """
    formatted_results = []

    for document in data:
        external_dataset = {}
        image_collections = 0
        external_repos = []

        now_utc = datetime.now(timezone.utc)
        external_dataset["timestamp"] = now_utc.isoformat(
            timespec="milliseconds"
        ).replace("+00:00", "Z")

        external_dataset["clinical_study_designation"] = document.get("entity_id")
        external_dataset["CRDCLinks"] = document.get("CRDCLinks", [])

        for link in external_dataset["CRDCLinks"]:
            image_collections += 1
            external_repos.append(link.get("repository"))

        external_dataset["numberOfImageCollections"] = image_collections
        external_dataset["numberOfCRDCNodes"] = len(set(external_repos))
        formatted_results.append(external_dataset)

    return formatted_results


@post_processor
def format_for_ccdi(data: list[dict]) -> list[dict]:
    """Formats fetched data for CCDI ingestion.

    Args:
        data (list[dict]): List of fetched data dicts.

    Returns:
        list[dict]: Formatted data ready for CCDI ingestion.
    """
    formatted_results = []

    now_utc = datetime.now(timezone.utc)
    timestamp = now_utc.isoformat(timespec="milliseconds").replace("+00:00", "Z")

    for document in data:
        formatted_results.append(
            {
                "timestamp": timestamp,
                "repository": document.get("repository", "unknown"),
                "data": document,
            }
        )

    return formatted_results
