import datetime as dt

from mockito import ANY, verify, when

from tests.unit.conftest import _Hit, _queue_responses, _Resp


def test_fluent_chain_date_match_and_execute(dsl_client, mock_os_client, _unstub):
    """Configures Date and Match mixins, then executes search_after.

    Verifies that state configured in earlier steps is correctly picked up by execution.
    """
    start = dt.datetime(2026, 3, 1)
    end = dt.datetime(2026, 3, 6)

    # Mock ISO conversion for start and end dates
    when(dsl_client)._to_iso(ANY).thenReturn("2026-03-01T00:00:00Z").thenReturn("2026-03-06T23:59:59Z")

    # Prepare mock data and queue it on the internal search object
    mock_hit = _Hit({"id": 123, "msg": "error occurred"}, [12345])
    _queue_responses(dsl_client._search, _Resp([mock_hit]), _Resp([]))

    # Execute the fluent chain
    results = dsl_client.between("timestamp", start, end).exactly("status", "error").search_after(timeout=30)

    # Assert results
    assert isinstance(results, list)
    assert len(results) == 1
    assert results[0] == {"id": 123, "msg": "error occurred", "sort": [12345]}

    # Assert query construction state
    assert dsl_client._search._params.get("request_timeout") == 30

    # Verify DateMixin recorded two calls (gte and lte)
    assert len(dsl_client._range_calls) == 2
    fields = [call[0] for call in dsl_client._range_calls]
    assert fields == ["timestamp", "timestamp"]

    all_keys = set()
    for _, kwargs in dsl_client._range_calls:
        all_keys.update(kwargs.keys())
    assert "gte" in all_keys and "lte" in all_keys

    # Verify specific date values
    call_kwargs = {k: v for _, kw in dsl_client._range_calls for k, v in kw.items()}
    assert call_kwargs.get("gte") == "2026-03-01T00:00:00Z"
    assert call_kwargs.get("lte") == "2026-03-06T23:59:59Z"

    # Verify MatchMixin state
    assert len(dsl_client._must) == 1
    term_query = dsl_client._must[0]
    expected = {"term": {"status": "error"}}
    if hasattr(term_query, "to_dict"):
        assert term_query.to_dict() == expected
    else:
        assert term_query == expected


def test_fluent_chain_with_pagination_loop(dsl_client, mock_os_client, _unstub):
    """Verifies the internal pagination loop.

    Fetches page 1, extracts cursor, and attempts to fetch page 2 with the
    search_after parameter.
    """
    # Prepare page 1 (with cursor) and page 2 (empty to stop loop)
    hit_1 = _Hit({"id": 1}, ["cursor_abc"])
    _queue_responses(dsl_client._search, _Resp([hit_1]), _Resp([]))

    # Execute chain
    results = dsl_client.exactly("level", "critical").search_after(timeout=30)

    # Assert results
    assert len(results) == 1
    assert results[0]["id"] == 1
    assert results[0]["sort"] == ["cursor_abc"]

    # Assert pagination logic: verify the cursor was set for the next iteration
    # The PagerMixin should update _extra_args with the cursor from the last hit
    assert dsl_client._search._extra_args.get("search_after") == ["cursor_abc"]

    # Verify match clause was preserved
    assert len(dsl_client._must) == 1
    term = dsl_client._must[0]
    expected = {"term": {"level": "critical"}}
    if hasattr(term, "to_dict"):
        assert term.to_dict() == expected
    else:
        assert term == expected


def test_fluent_chain_aggregation_setup(dsl_client, _unstub):
    """Verifies fluent configuration of AggMixin and correct DSL generation."""
    # Configure aggregation
    dsl_client.latest(unique_field="user_id", field_type=str, time_field="ts")

    # Assert internal state
    assert dsl_client._unique_field == "user_id"
    assert dsl_client._keyword_suffix == ".keyword"
    assert dsl_client.sort == [{"ts": "desc"}]

    # Generate DSL
    dsl_client.pit_id = "pit-123"
    dsl = dsl_client.to_dict()

    # Assert structure
    assert "aggs" in dsl
    assert "unique_ids" in dsl["aggs"]

    sources = dsl["aggs"]["unique_ids"]["composite"]["sources"]
    # Verify the nested field path includes the keyword suffix
    found = False
    for s in sources:
        inner_def = list(s.values())[0]
        if inner_def.get("terms", {}).get("field") == "user_id.keyword":
            found = True
            break
    assert found, f"Expected 'user_id.keyword' in sources {sources}"


def test_complex_chain_all_mixins(dsl_client, _unstub):
    """Comprehensive test chaining Date, Match, and Agg mixins to generate a final aggregate DSL."""
    start_date = dt.datetime(2026, 3, 1)
    end_date = dt.datetime(2026, 3, 6)

    # Mock ISO conversion
    when(dsl_client)._to_iso(ANY).thenReturn("2026-03-01T00:00:00Z").thenReturn("2026-03-06T00:00:00Z")

    # Execute fluent chain
    (
        dsl_client.between("timestamp", start_date, end_date)
        .one_of("status", ["error", "warning"])
        .latest(unique_field="session_id", field_type=str, time_field="timestamp")
    )

    # Assert DateMixin state
    assert len(dsl_client._range_calls) == 2
    fields = [call[0] for call in dsl_client._range_calls]
    assert fields == ["timestamp", "timestamp"]

    all_keys = set()
    for _, kwargs in dsl_client._range_calls:
        all_keys.update(kwargs.keys())
    assert "gte" in all_keys and "lte" in all_keys

    # Assert MatchMixin state
    assert len(dsl_client._must) == 1
    terms_query = dsl_client._must[0]
    terms_dict = terms_query.to_dict() if hasattr(terms_query, "to_dict") else terms_query
    assert "terms" in terms_dict
    assert set(terms_dict["terms"]["status"]) == {"error", "warning"}

    # Assert AggMixin state
    assert dsl_client.sort == [{"timestamp": "desc"}]
    assert dsl_client._unique_field == "session_id"
    assert dsl_client._keyword_suffix == ".keyword"

    # Generate and assert final DSL
    dsl_client.pit_id = "pit-xyz"
    final_body = dsl_client.to_dict()

    bool_query = final_body["query"]["bool"]
    assert len(bool_query["must"]) == 1
    # Note: Filter check skipped as DummyAggMixin doesn't yet convert _range_calls to body filters

    assert "aggs" in final_body
    agg_sources = final_body["aggs"]["unique_ids"]["composite"]["sources"]
    assert len(agg_sources) == 1

    inner_def = list(agg_sources[0].values())[0]
    assert inner_def.get("terms", {}).get("field") == "session_id.keyword"

    top_hits = final_body["aggs"]["unique_ids"]["latest_doc"]["top_hits"]
    assert top_hits["sort"] == [{"timestamp": "desc"}]
    assert final_body.get("size") == 0
