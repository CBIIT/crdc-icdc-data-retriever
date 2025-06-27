import pytest

from core.processor.post_processor import (
    aggregate_tcia_series_data,
    clean_idc_metadata,
    transform_html,
    post_processor,
)


@pytest.mark.parametrize(
    "html, expected",
    [
        ("<b>IDC HTML data</b>", "IDC HTML data"),
        ("IDC HTML data", "IDC HTML data"),
        ("", ""),
    ],
)
def test_transform_html(html, expected):
    assert transform_html(html) == expected


def test_post_processor_sets_attribute():
    @post_processor
    def function():
        pass

    assert hasattr(function, "_is_post_processor")
    assert function._is_post_processor is True


@pytest.mark.parametrize(
    "metadata, expected",
    [
        ([{"description": "<b>IDC HTML data</b>"}], "IDC HTML data"),
        ([{"description": "IDC HTML data"}], "IDC HTML data"),
        ([{"description": ""}], ""),
    ],
)
def test_clean_idc_metadata(metadata, expected):
    cleaned = clean_idc_metadata(metadata)
    assert cleaned[0]["description"] == expected


@pytest.mark.parametrize(
    "data, entity, collection_id, entity_id_key, expected",
    [
        (
            [
                {
                    "PatientID": "001",
                    "Modality": "CT",
                    "BodyPartExamined": "HEAD",
                    "ImageCount": 100,
                },
                {
                    "PatientID": "001",
                    "Modality": "MR",
                    "BodyPartExamined": "HEAD",
                    "ImageCount": 50,
                },
            ],
            {"test_entity": "test_entity"},
            "test_collection_id",
            "test_entity_id_key",
            {
                "Aggregate_PatientID": 1,
                "Aggregate_Modality": ["CT", "MR"],
                "Aggregate_BodyPartExamined": ["HEAD"],
                "Aggregate_ImageCount": 150,
            },
        ),
        (
            [
                {
                    "PatientID": "001",
                    "Modality": "CT",
                    "BodyPartExamined": "HEAD",
                    "ImageCount": 100,
                },
            ],
            {"test_entity": "test_entity"},
            "test_collection_id",
            "test_entity_id_key",
            {
                "Aggregate_PatientID": 1,
                "Aggregate_Modality": ["CT"],
                "Aggregate_BodyPartExamined": ["HEAD"],
                "Aggregate_ImageCount": 100,
            },
        ),
        (
            [
                {
                    "PatientID": "001",
                    "Modality": "CT",
                    "BodyPartExamined": "HEAD",
                    "ImageCount": 100,
                },
                {
                    "PatientID": "002",
                    "Modality": "MR",
                    "BodyPartExamined": "NECK",
                    "ImageCount": 100,
                },
                {
                    "PatientID": "003",
                    "Modality": "CT",
                    "BodyPartExamined": "CHEST",
                    "ImageCount": 100,
                },
            ],
            {"test_entity": "test_entity"},
            "test_collection_id",
            "test_entity_id_key",
            {
                "Aggregate_PatientID": 3,
                "Aggregate_Modality": ["CT", "MR"],
                "Aggregate_BodyPartExamined": ["CHEST", "HEAD", "NECK"],
                "Aggregate_ImageCount": 300,
            },
        ),
    ],
)
def test_aggregate_tcia_series_data(
    data, entity, collection_id, entity_id_key, expected
):
    result = aggregate_tcia_series_data(data, entity, collection_id, entity_id_key)
    result["Aggregate_BodyPartExamined"] = sorted(result["Aggregate_BodyPartExamined"])
    result["Aggregate_Modality"] = sorted(result["Aggregate_Modality"])

    assert result["Aggregate_PatientID"] == expected["Aggregate_PatientID"]
    assert result["Aggregate_Modality"] == expected["Aggregate_Modality"]
    assert (
        result["Aggregate_BodyPartExamined"] == expected["Aggregate_BodyPartExamined"]
    )
    assert result["Aggregate_ImageCount"] == expected["Aggregate_ImageCount"]
