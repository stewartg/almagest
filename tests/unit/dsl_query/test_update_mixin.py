from mockito import verify, when, when2
from opensearchpy.exceptions import NotFoundError

from tests.unit.conftest import _Hit, _queue_responses, _Resp


def test_get_by_id_success(dsl_client, mock_os_client, _unstub):
    """Verifies fetching a document by ID returns the _source."""
    doc_id = "doc-123"
    mock_source = {"id": 123, "message": "hello world"}
    mock_response = {"_source": mock_source, "_id": doc_id, "found": True}

    when(mock_os_client).get(index=dsl_client.index, id=doc_id).thenReturn(mock_response)

    result = dsl_client.get_by_id(doc_id)

    assert result == mock_source
    verify(mock_os_client).get(index=dsl_client.index, id=doc_id)


def test_get_by_id_not_found(dsl_client, mock_os_client, _unstub):
    """Verifies fetching a non-existent document returns None."""
    doc_id = "doc-missing"

    when(mock_os_client).get(index=dsl_client.index, id=doc_id).thenRaise(NotFoundError(404, "Not Found"))

    result = dsl_client.get_by_id(doc_id)

    assert result is None
    verify(mock_os_client).get(index=dsl_client.index, id=doc_id)


def test_get_id_by_field_success(dsl_client, mock_os_client, _unstub):
    """Verifies finding an ID by a unique field using opensearch_dsl Search."""
    field = "email"
    value = "user@example.com"
    found_id = "user-uuid-999"

    mock_hit = _Hit({"email": value}, [], doc_id=found_id)

    # 1. Configure the response on the client's shared stub
    _queue_responses(dsl_client._search, _Resp([mock_hit]), _Resp([]))

    # mock the opensearch_dsl Search object that gets created in the get_id_by_field
    when2("opensearch_dsl.Search.__new__", index=dsl_client.index, using=dsl_client._client).thenReturn(
        dsl_client._search
    )
    result = dsl_client.get_id_by_field(field, value)

    assert result == found_id
    # verify query() was called
    assert dsl_client._search._query_args is not None


def test_get_id_by_field_no_results(dsl_client, mock_os_client, _unstub):
    """Verifies returning None when no document matches the field."""
    field = "token"
    value = "invalid-token"

    _queue_responses(dsl_client._search, _Resp([]), _Resp([]))

    # mock the opensearch_dsl Search object that gets created in the get_id_by_field
    when2("opensearch_dsl.Search.__new__", index=dsl_client.index, using=dsl_client._client).thenReturn(
        dsl_client._search
    )
    result = dsl_client.get_id_by_field(field, value)

    assert result is None


def test_update_record_success(dsl_client, mock_os_client, _unstub):
    """Verifies partial update with refresh and retry logic."""
    doc_id = "doc-456"
    update_body = {"status": "resolved"}
    mock_response = {"result": "updated", "_version": 2}

    when(mock_os_client).update(
        index=dsl_client.index, id=doc_id, body={"doc": update_body}, params={"refresh": "true", "retry_on_conflict": 3}
    ).thenReturn(mock_response)

    result = dsl_client.update_record(doc_id, update_body, refresh=True)

    assert result == mock_response
    verify(mock_os_client).update(
        index=dsl_client.index, id=doc_id, body={"doc": update_body}, params={"refresh": "true", "retry_on_conflict": 3}
    )


def test_upsert_record_with_defaults(dsl_client, mock_os_client, _unstub):
    """Verifies upsert uses separate default_body for creation vs update_body."""
    doc_id = "new-doc-222"
    update_body = {"last_seen": "2026-03-10"}
    default_body = {"user": "bob", "created_at": "2026-01-01"}

    expected_body = {"doc": update_body, "doc_as_upsert": True, "upsert": default_body}

    mock_response = {"result": "created"}
    when(mock_os_client).update(
        index=dsl_client.index, id=doc_id, body=expected_body, params={"refresh": "false"}
    ).thenReturn(mock_response)
    result = dsl_client.upsert_record(doc_id, update_body, default_body=default_body)

    assert result == mock_response
    verify(mock_os_client).update(index=dsl_client.index, id=doc_id, body=expected_body, params={"refresh": "false"})


def test_fluent_chain_update_integration(dsl_client, mock_os_client, _unstub):
    """Integrates MatchMixin to find a record, then UpdateMixin to modify it."""
    target_id = "task-999"
    mock_hit = _Hit({"status": "pending"}, [], doc_id=target_id)
    _queue_responses(dsl_client._search, _Resp([mock_hit]), _Resp([]))

    update_data = {"status": "completed"}
    mock_update_resp = {"result": "updated"}

    # FIX: Wrap update_data in {"doc": ...} to match the real implementation
    expected_body = {"doc": update_data}

    when(mock_os_client).update(
        index=dsl_client.index,
        id=target_id,
        body=expected_body,  # Changed from update_data to expected_body
        params={"refresh": "false", "retry_on_conflict": 3},
    ).thenReturn(mock_update_resp)

    # Mock the Search constructor
    when2("opensearch_dsl.Search.__new__", index=dsl_client.index, using=dsl_client._client).thenReturn(
        dsl_client._search
    )

    # Chain: Filter -> Find ID -> Update
    found_id = dsl_client.exactly("status", "pending").get_id_by_field("status", "pending")
    assert found_id == target_id

    result = dsl_client.update_record(found_id, update_data)
    assert result == mock_update_resp

    # Verify MatchMixin state
    assert len(dsl_client._must) == 1
    term_q = dsl_client._must[0]
    term_dict = term_q.to_dict() if hasattr(term_q, "to_dict") else term_q
    assert term_dict == {"term": {"status": "pending"}}
