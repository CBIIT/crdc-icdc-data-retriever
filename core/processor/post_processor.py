import logging
import re

from html2text import HTML2Text

logger = logging.getLogger(__name__)


def post_processor(fn):
    fn._is_post_processor = True
    return fn


def transform_html(html: str) -> str:
    # handle oddly-formatted IDC response HTML
    converter = HTML2Text()
    converter.ignore_links = True
    converter.body_width = 0
    converter.ignore_emphasis = True
    converter.single_line_break = True

    text = converter.handle(html)
    text = re.sub(r"\s+", " ", text).strip()

    return text


@post_processor
def clean_idc_metadata(metadata_list: list) -> list:
    for metadata in metadata_list:
        if "description" in metadata:
            metadata["description"] = transform_html(metadata["description"])
            logger.info("Transformed HTML in 'description' field of metadata")
        else:
            logger.warning("'description' key not found in metadata.")
    return metadata_list


@post_processor
def aggregate_tcia_series_data(
    data: list, entity: str, collection_id: str, entity_id_key: str
) -> dict:
    total_images = 0
    total_patients = set()
    unique_modalities = set()
    unique_bodyparts = set()

    for item in data:
        total_images += item["ImageCount"]
        total_patients.add(item["PatientID"])
        unique_modalities.add(item["Modality"])
        unique_bodyparts.add(item["BodyPartExamined"])

    # hardcode inaccessible TCIA data for GLIOMA01
    entity_id = entity.get(entity_id_key)
    if entity_id == "GLIOMA01":
        unique_modalities.add("Histopathology")
        total_images += 84
        logger.info("Hardcoded TCIA data for GLIOMA01 entity added to totals.")

    logger.info(
        f"Completed aggregation of TCIA series data for collection '{collection_id}': "
        f"{len(total_patients)} patients, {total_images} images, "
        f"modalities: {sorted(unique_modalities)}, body parts: {sorted(unique_bodyparts)}"
    )

    return {
        "Collection": collection_id,
        "Aggregate_PatientID": len(total_patients),
        "Aggregate_Modality": list(unique_modalities),
        "Aggregate_BodyPartExamined": list(unique_bodyparts),
        "Aggregate_ImageCount": total_images,
    }
