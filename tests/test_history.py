import json
import os
import pytest

from spl.history import SplHistoryResponse, get_spl, process_spl_history

TEST_DATA_DIR = os.path.join("tests", "testdata")
TEST_SET_ID = "9525f887-a055-4e33-8e92-898d42828cd1"


class MockResponse:
    def __init__(self, content):
        self.content = content

    def json(self):
        return self.content


def _read_setid_history_baseline():
    with open(os.path.join(TEST_DATA_DIR, "test_history.json")) as f:
        return json.loads(f.read())


@pytest.fixture
def mock_fetch_and_process(monkeypatch):
    def mock_method(self):
        self.called = True
        self.data = {"metadata": {"total_pages": "30"}}

    monkeypatch.setattr(SplHistoryResponse, "_fetch_and_process", mock_method)


@pytest.fixture
def mock_request(monkeypatch):
    def mock_method(url, allow_redirects):
        """This method expects to have been invoked with specific args, without
        which it will return None
        """
        if (
            url
            == "https://dailymed.nlm.nih.gov/dailymed/services/v2/spls/9525f887-a055-4e33-8e92-898d42828cd1/history"
            and allow_redirects == True
        ):
            content = None
            with open(os.path.join(TEST_DATA_DIR, "test_history.json")) as f:
                content = f.read()
            return MockResponse(content)


def test_class_attributes():
    assert (
        SplHistoryResponse.BASE_URL
        == "https://dailymed.nlm.nih.gov/dailymed/services/v2/spls"
    )
    assert SplHistoryResponse.RESOURCE_PATH == "history"


def test_init_method(mock_fetch_and_process):
    # Check invalid initialization
    with pytest.raises(ValueError):
        _ = SplHistoryResponse(None)

    # Check valid initialization
    spl_history = SplHistoryResponse("test-setid")
    # Assert setid and that _fetch_and_process is called
    assert spl_history.set_id == "test-setid"
    assert spl_history.called == True


def test_get_total_pages(mock_fetch_and_process):
    spl_history = SplHistoryResponse("test-setid")
    assert spl_history.get_total_pages() == 30


def test_fetch_and_process(mock_request):
    spl_obj = SplHistoryResponse(TEST_SET_ID)
    # Test against baseline
    data = _read_setid_history_baseline()
    spl_obj.data == data


def test_get_spl(mock_request):
    spl_history = get_spl(TEST_SET_ID)
    # Test against baseline
    data = _read_setid_history_baseline()
    spl_history == data


def test_process_spl_history(mock_request):
    spl_history = process_spl_history([TEST_SET_ID])
    # Test against baseline
    data = _read_setid_history_baseline()
    assert spl_history == [data]
