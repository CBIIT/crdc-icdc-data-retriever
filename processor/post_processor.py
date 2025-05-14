import re
from html2text import HTML2Text


def post_processor(fn):
    fn._is_post_processor = True
    return fn


@post_processor
def transform_idc_description_html(html: str) -> str:
    # Python version of node.js getIdcCollectionMetadata
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
def aggregate_tcia_series_data(data: list, study: str, collection_id: str) -> dict:
    # Python version of node.js getTciaCollectionMetadata
    total_images = sum(int(i["ImageCount"]) for i in data)
    total_patients = len(set([i["PatientID"] for i in data]))
    unique_modalities = list(set([i["Modality"] for i in data]))
    unique_bodyparts = list(set([i["BodyPartExamined"] for i in data]))

    # hardcode inaccessible TCIA data for GLIOMA01
    if study == "GLIOMA01":
        unique_modalities.append("Histopathology")
        total_images += 84

    return {
        "Collection": collection_id,
        "Aggregate_PatientID": total_patients,
        "Aggregate_Modality": unique_modalities,
        "Aggregate_BodyPartExamined": unique_bodyparts,
        "Aggregate_ImageCount": total_images,
    }
