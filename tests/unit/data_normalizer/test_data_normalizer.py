import json
from pathlib import Path

import pytest

from almagest.data_normalizer import DataNormalizer
from tests.unit.conftest import get_test_data_dir


@pytest.fixture
def data_source_params() -> dict:
    """Creates data structure with parameters necessary for data standardization.

    :return: a dictionary containing the 'index_details' and 'standard_params' keys
    and the corresponding required parameters
    """
    return {
        "index_details": {"alias": "dnd_udl_elsets", "index_partition_date_format": "none", "index_version": 1},
        "standard_params": {
            "classification_field": "classificationMarking",
            "start_date_field": "epoch",
            "provider_details": {
                "dataProvider": "udl_elsets",
                "dataProviderUrl": "https://unifieddatalibrary.com",
            },
            "unique_fields": ["idElset"],
        },
    }


@pytest.fixture(params=get_test_data_dir("data_normalizer"))
def elsets_data(request: pytest.FixtureRequest) -> dict[str, dict]:
    """Loads a sample elsets data set.

    :return: The loaded vcat JSON file
    """
    with Path.open(f"{request.param}/udl_elsets.json", encoding="utf-8") as fin:
        elsets_data = json.load(fin)
        return elsets_data


@pytest.fixture
def data_normalizer(elsets_data, data_source_params):
    return DataNormalizer(elsets_data, **data_source_params)


def test_standardize_data(data_normalizer: DataNormalizer, data_source_params: dict, _unstub):
    """Tests that the normalizer does the following.

    1. Removes fields that consist of only spaces or dashes and replaces them with empty string.
    2. Renames "classificationMarking" field to "classification".
    3. Renames the "epoch" field to "startTimestamp" and doesn't remove dash characters from
       strings that contain a dash and other characters.
    4. Ensures that fields with multiple spaces are preserved, ie line1, line2.
    :param data_normalizer: the DataNormalizer instance.
    :param standard_params: a dictionary containing the parameters necessary to standardize
    udl elsets data.
    """
    standard_params = data_source_params["standard_params"]
    data_df = data_normalizer.standardize(**standard_params)
    elsets_cols = list(data_df.columns)
    assert "epoch" not in elsets_cols
    assert "classificationMarking" not in elsets_cols
    elsets_data = data_df.to_dict("records")
    assert elsets_data[0]["bogus_field1"] == ""
    assert elsets_data[0]["bogus_field2"] == ""
    assert elsets_data[3]["classification"] == "U//PR-EXO-ELSETS"
    assert elsets_data[2]["startTimestamp"] == "2024-07-31T00:59:57.795360Z"
    assert elsets_data[4]["line1"] == "1 21135U 91014D   24213.04162301 +.00000000 +00000+0 +00000+0 0 99993"
    assert elsets_data[4]["line2"] == "2 21135  12.6438 336.9632 0027426 133.5005 230.1678 01.03426449000008"
