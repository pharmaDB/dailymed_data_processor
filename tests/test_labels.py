import json
import os
import pytest

from spl.labels import (
    SplHistoricalLabels,
    process_labels_for_set_id,
    process_historical_labels,
)

TEST_DATA_DIR = os.path.join("tests", "testdata")
TEMPDATA_DIR = "tempdata"
TEST_SET_ID = "1b5e2860-6855-4a65-8bbc-e064172a1adf"
TEST_SET_SPL_VERSION = 1


class MockResponse:
    def __init__(self, content):
        self.content = content


def _read_label_baseline():
    with open(os.path.join(TEST_DATA_DIR, "baselines", "test_label.json")) as f:
        return json.loads(f.read())


@pytest.fixture
def mock_fetch_and_process(monkeypatch):
    def mock_method(self):
        self.called = True

    monkeypatch.setattr(SplHistoricalLabels, "_fetch_and_process", mock_method)


@pytest.fixture
def mock_request(monkeypatch):
    def mock_method(url, allow_redirects):
        """This method expects to have been invoked with specific args, without
        which it will return None
        """
        if (
            url
            == "https://dailymed.nlm.nih.gov/dailymed/getFile.cfm?type=zip&setid=1b5e2860-6855-4a65-8bbc-e064172a1adf&version=1"
            and allow_redirects == True
        ):
            content = None
            with open(
                os.path.join(
                    TEST_DATA_DIR, "1b5e2860-6855-4a65-8bbc-e064172a1adf_1.zip"
                ),
                "rb",
            ) as f:
                content = f.read()
            return MockResponse(content)


def test_class_attributes():
    assert (
        SplHistoricalLabels.BASE_URL
        == "https://dailymed.nlm.nih.gov/dailymed/getFile.cfm?type=zip"
    )
    assert SplHistoricalLabels.LABEL_SECTIONS == [
        "INDICATIONS AND USAGE",
        "DOSAGE FORMS AND STRENGTHS",
        "DESCRIPTION",
        "INDICATIONS",
        "ACTIVE INGREDIENT",
        "INACTIVE INGREDIENTS",
        "PURPOSE",
        "DIRECTIONS",
        "USE",
    ]


def test_init_method(mock_fetch_and_process):
    # Check invalid initialization
    with pytest.raises(ValueError):
        _ = SplHistoricalLabels(None, TEMPDATA_DIR)
    with pytest.raises(ValueError):
        _ = SplHistoricalLabels({}, None)
    with pytest.raises(ValueError):
        _ = SplHistoricalLabels({}, TEMPDATA_DIR)

    # Check valid initialization
    spl_data = {
        "data": {
            "spl": {"setid": "test-setid"},
            "history": [{"spl_version": 1}],
        }
    }
    spl_history = SplHistoricalLabels(spl_data, TEMPDATA_DIR)
    # Assert setid and that _fetch_and_process is called
    assert spl_history.download_path == TEMPDATA_DIR
    assert spl_history.called == True


def test_fetch_and_process(mock_request):
    spl_data = {
        "data": {
            "spl": {"setid": TEST_SET_ID},
            "history": [{"spl_version": TEST_SET_SPL_VERSION}],
        }
    }
    labels = SplHistoricalLabels(spl_data, TEMPDATA_DIR)

    assert labels.application_numbers_for_setid == set(["21812"])
    assert labels.spl_label_versions == _read_label_baseline()
