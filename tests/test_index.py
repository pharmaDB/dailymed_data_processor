import json
import os
import pytest

from spl.index import SplIndexFile

TEST_DATA_DIR = os.path.join("tests", "testdata")


class MockResponse:
    def __init__(self, content):
        self.content = content


@pytest.fixture
def mock_fetch_and_process(monkeypatch):
    def mock_method(self):
        self.called = True
        self.metadata = {"total_pages": 500}

    monkeypatch.setattr(SplIndexFile, "_fetch_and_process", mock_method)


@pytest.fixture
def mock_request(monkeypatch):
    def mock_method(url, allow_redirects):
        if (
            url
            == "https://dailymed.nlm.nih.gov/dailymed/services/v2/spls.xml?page=1"
            and allow_redirects == True
        ):
            content = None
            with open(os.path.join(TEST_DATA_DIR, "test_index_page.xml")) as f:
                content = f.read()
            return MockResponse(content)


def test_class_attribute():
    assert (
        SplIndexFile.BASE_URL
        == "https://dailymed.nlm.nih.gov/dailymed/services/v2/spls.xml"
    )


def test_init_method(mock_fetch_and_process):
    # Check invalid initialization
    with pytest.raises(ValueError):
        _ = SplIndexFile("bad_value")

    # Check valid initialization
    spl_obj = SplIndexFile(100)
    # Assert page number and that _fetch_and_process is called
    assert spl_obj.page_number == 100
    assert spl_obj.called == True


def test_get_max_page_number(mock_fetch_and_process):
    spl_obj = SplIndexFile(100)
    assert spl_obj.get_max_page_number() == 500


def test_fetch_and_process(mock_request):
    spl_obj = SplIndexFile(1)

    # Test against baseline
    data = None
    with open(
        os.path.join(TEST_DATA_DIR, "baselines", "test_index_page.json")
    ) as f:
        data = json.loads(f.read())
    assert spl_obj.metadata == data["metadata"]
    assert list(map(lambda x: dict(x), spl_obj.spls)) == data["spls"]
