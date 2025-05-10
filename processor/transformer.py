from functools import reduce
from html2text import HTML2Text


def transform_idc_description_html(html: str) -> str:
    # Python version of node.js getIdcCollectionMetadata
    # handle oddly-formatted IDC response HTML
    converter = HTML2Text()
    converter.ignore_links = True
    converter.body_width = 0
    converter.ignore_emphasis = True
    converter.single_line_break = True

    text = converter.handle(html)
    text = text.replace("\n", " ").replace("  ", " ").replace("    ", " ")

    return text.strip()


def aggregate_tcia_series_data(data: list) -> dict:
    # Python version of node.js getTciaCollectionMetadata
    total_images = reduce(lambda x, y: x + y, [int(i["ImageCount"]) for i in data])
    total_patients = len(set([i["PatientID"] for i in data]))
    unique_modalities = list(set([i["Modality"] for i in data]))
    unique_bodyparts = list(set([i["BodyPartExamined"] for i in data]))

    # hardcode inaccessible TCIA data for GLIOMA01...
    # determine how study can be passed to this
    # so additional values can be appended for it and only it...

    return {}
