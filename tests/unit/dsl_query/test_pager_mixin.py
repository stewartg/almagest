from mockito import when
from opensearch_dsl import Index

from tests.unit.conftest import DummyPagerMixin, _Hit, _queue_responses, _Resp


def test_search_after_paginates_multiple_pages(pager_mixin: DummyPagerMixin, _unstub: None) -> None:
    """Verify search_after() correctly iterates through multiple pages of results.

    This test simulates a multi-page pagination scenario by queuing two pages of
    data followed by an empty page (signaling the end). It verifies that
    `search_after()` correctly aggregates the results from all pages into a single
    flat list, extracting both the source document and the sort keys for each hit.

    Assertions:
    - The result is a flat list containing hits from both Page 1 and Page 2.
    - Each item in the result includes both the source data (`id`) and the `sort` key.
    - The loop terminates correctly when an empty page is encountered.
    """
    # two pages of hits, then an empty page to stop the loop
    page1 = [_Hit({"id": 1}, ["s1"]), _Hit({"id": 2}, ["s2"])]
    page2 = [_Hit({"id": 3}, ["s3"])]
    _queue_responses(pager_mixin._search, _Resp(page1), _Resp(page2), _Resp([]))

    result = pager_mixin.search_after(timeout=120)

    # expected flat list of the `_source` dicts from page1 + page2
    assert result == [
        {"id": 1, "sort": ["s1"]},
        {"id": 2, "sort": ["s2"]},
        {"id": 3, "sort": ["s3"]},
    ]


def test_get_sample_record(pager_mixin: DummyPagerMixin, _unstub: None) -> None:
    """Verify get_sample_record() retrieves and formats a single document.

    This test ensures that `get_sample_record()` fetches exactly one hit from
    the search results and returns it in the expected format (source data merged
    with sort keys). It validates the method's ability to extract a representative
    record for inspection or debugging.

    Assertions:
    - The result matches the source data of the single mock hit.
    - The `sort` keys are included in the returned dictionary.
    """
    # Setup a single hit response with the expected structure
    sample_hit = _Hit({"id": "test-id", "name": "test"}, ["s4"])
    _queue_responses(pager_mixin._search, _Resp([sample_hit]))

    result = pager_mixin.get_sample_record()

    # Expected result should be just the _source part merged with sort
    assert result == {"id": "test-id", "name": "test", "sort": ["s4"]}


def test_get_mappings(pager_mixin: DummyPagerMixin, _unstub: None) -> None:
    """Verify get_mappings() retrieves the index mapping definition.

    This test mocks the `Index.get_mapping()` method to return a predefined
    mapping structure. It verifies that `pager.get_mappings()` correctly
    delegates to the OpenSearch DSL client and returns the raw mapping dictionary.

    Assertions:
    - The returned result matches the mocked mapping structure exactly.
    - The `Index.get_mapping` method is called (implicitly verified by the mock).
    """
    expected_mapping = {"test_index": {"mappings": {"properties": {"field1": {"type": "text"}}}}}
    when(Index).get_mapping().thenReturn(expected_mapping)

    result = pager_mixin.get_mappings()
    assert result == expected_mapping


def test_get_template(pager_mixin: DummyPagerMixin, _unstub: None) -> None:
    """Verify get_template() retrieves the index template definition.

    This test mocks the client's `indices.get_template()` method to return a
    predefined template configuration. It verifies that `pager.get_template()`
    correctly accesses the client interface and returns the template dictionary.

    Assertions:
    - The returned result matches the mocked template structure exactly.
    - The `client.indices.get_template` method is called with appropriate arguments.
    """
    template = {"sample_data_source": {"template": {"settings": {"number_of_shards": 1}}}}
    when(pager_mixin._client.indices).get_template(...).thenReturn(template)

    result = pager_mixin.get_template()
    assert result == template
