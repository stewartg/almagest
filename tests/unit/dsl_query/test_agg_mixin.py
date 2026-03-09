import pytest
from mockito import ANY, verify, when

from tests.unit.conftest import DummyAggMixin


def test_latest_sets_fields_and_descending(agg_mixin: DummyAggMixin, _unstub) -> None:
    """Verify that latest() configures state and triggers descending sort.

    This test ensures that calling `latest()` correctly sets the `_unique_field`,
    determines the correct `_keyword_suffix` based on the field type (int vs str),
    records the time field in the mock `descending` helper, and updates the `sort`
    attribute to descending order.

    Assertions:
    - The method returns `self` for chaining.
    - `_unique_field` matches the input.
    - `_keyword_suffix` is empty for integers.
    - The recorded descending field matches the input time field.
    - The `sort` list reflects the descending order.
    """
    result = agg_mixin.latest(
        unique_field="user_id",
        field_type=int,
        time_field="created_at",
    )
    assert result is agg_mixin
    assert agg_mixin._unique_field == "user_id"
    assert agg_mixin._keyword_suffix == ""  # int → no .keyword
    assert agg_mixin._recorded_desc == ["created_at"]
    assert agg_mixin.sort == [{"created_at": {"order": "desc"}}]


def test_earliest_sets_fields_and_ascending(agg_mixin: DummyAggMixin, _unstub) -> None:
    """Verify that earliest() configures state and triggers ascending sort.

    This test ensures that calling `earliest()` correctly sets the `_unique_field`,
    appends the `.keyword` suffix for string fields, records the time field in
    the mock `ascending` helper, and updates the `sort` attribute to ascending order.

    Assertions:
    - The method returns `self` for chaining.
    - `_unique_field` matches the input.
    - `_keyword_suffix` is ".keyword" for strings.
    - The recorded ascending field matches the input time field.
    - The `sort` list reflects the ascending order.
    """
    result = agg_mixin.earliest(
        unique_field="email",
        field_type=str,
        time_field="updated_at",
    )
    assert result is agg_mixin
    assert agg_mixin._unique_field == "email"
    assert agg_mixin._keyword_suffix == ".keyword"
    assert agg_mixin._recorded_asc == ["updated_at"]
    assert agg_mixin.sort == [{"updated_at": {"order": "asc"}}]


def test_to_dict_routes_to_aggregate_when_configured(agg_mixin: DummyAggMixin, _unstub: None) -> None:
    """Verify to_dict() delegates to _build_aggregate_query when PIT is set.

    This test checks the routing logic of `to_dict()`. When a `pit_id` is present
    and an aggregation is configured, it should delegate to `_build_aggregate_query`.
    We mock the internal method to ensure it is called exactly once and its return
    value is propagated.

    Assertions:
    - The output matches the mocked return value of `_build_aggregate_query`.
    - `_build_aggregate_query` is verified to have been called exactly once.
    """
    agg_mixin.pit_id = "pit-123"
    agg_mixin.latest("uid", int, "ts")
    sentinel = {"agg": "payload"}
    when(agg_mixin)._build_aggregate_query().thenReturn(sentinel)

    out = agg_mixin.to_dict()
    assert out is sentinel
    verify(agg_mixin, times=1)._build_aggregate_query()


def test_to_dict_falls_back_to_standard_when_no_agg(agg_mixin: DummyAggMixin, _unstub: None) -> None:
    """Verify to_dict() falls back to _build_standard_query without PIT.

    This test checks the fallback logic of `to_dict()`. When `pit_id` is None,
    the method should delegate to `_build_standard_query` instead of the aggregate
    builder. We mock the internal method to verify this routing.

    Assertions:
    - The output matches the mocked return value of `_build_standard_query`.
    - `_build_standard_query` is verified to have been called exactly once.
    """
    agg_mixin.pit_id = None
    sentinel = {"search": "standard"}
    when(agg_mixin)._build_standard_query().thenReturn(sentinel)

    out = agg_mixin.to_dict()
    assert out is sentinel
    verify(agg_mixin, times=1)._build_standard_query()


def test_build_aggregate_query_raises_without_unique_field(agg_mixin: DummyAggMixin, _unstub: None) -> None:
    """Verify _build_aggregate_query raises ValueError if unique_field is missing.

    This test ensures that calling `_build_aggregate_query` without first configuring
    the aggregation via `latest()` or `earliest()` (which set `_unique_field`)
    results in a `ValueError`.

    Assertions:
    - A `ValueError` is raised with the expected message substring.
    """
    agg_mixin.pit_id = "some-pit"
    # Do NOT call latest()/earliest() → _unique_field stays None.
    with pytest.raises(ValueError, match="unique_field must be set"):
        agg_mixin._build_aggregate_query()


def test_build_aggregate_query_structure(agg_mixin: DummyAggMixin, _unstub) -> None:
    """Verify the detailed structure of the generated aggregate query.

    This test validates the full dictionary structure produced by
    `_build_aggregate_query` after configuring `latest()`. It checks for the
    presence of top-level keys, the correct nesting of composite and top_hits
    aggregations, the proper field naming (with or without `.keyword`), the
    inclusion of the PIT configuration, and the passing of the `after_key`
    for pagination.

    Assertions:
    - `size` is 0.
    - The `aggs` hierarchy contains `unique_ids` -> `composite` and `latest_doc`.
    - The composite source field matches the unique field (no suffix for int).
    - The top_hits aggregation includes the correct sort order.
    - The `pit` block contains the ID and keep_alive setting.
    - The `after` key in the composite agg matches the input `after_key`.
    """
    agg_mixin.pit_id = "pit-xyz"
    agg_mixin.after_key = {"uid": "abc"}
    agg_mixin.latest("uid", int, "ts")  # sets sort & keyword suffix

    result = agg_mixin._build_aggregate_query()

    # Top-level keys
    assert result["size"] == 0
    assert "aggs" in result
    assert "unique_ids" in result["aggs"]
    assert "composite" in result["aggs"]["unique_ids"]
    assert "latest_doc" in result["aggs"]["unique_ids"]

    # Composite source must reference the correct field (int → no .keyword)
    comp_source = result["aggs"]["unique_ids"]["composite"]["sources"][0]
    assert "uid" in comp_source
    assert comp_source["uid"]["terms"]["field"] == "uid"

    # Top-hits aggregation must contain the sort we set via latest()
    top_hits = result["aggs"]["unique_ids"]["latest_doc"]["top_hits"]
    assert top_hits["size"] == 1
    assert top_hits["sort"] == [{"ts": {"order": "desc"}}]

    # PIT block
    assert result["pit"]["id"] == "pit-xyz"
    assert result["pit"]["keep_alive"] == "5m"

    # after_key should be passed through unchanged
    assert result["aggs"]["unique_ids"]["composite"]["after"] == {"uid": "abc"}


def test_agg_mixin_chaining_latest_then_earliest(agg_mixin: DummyAggMixin, _unstub) -> None:
    """Verify chaining latest() then earliest() overwrites state correctly.

    This test demonstrates that `latest()` and `earliest()` can be chained.
    It verifies that the second call (`earliest`) overwrites the aggregation-specific
    state (`_unique_field`, `_keyword_suffix`, `sort`) while preserving the history
    of recorded calls to the sorting helpers (e.g., the `descending` call from
    `latest` remains in `_recorded_desc`).

    Assertions:
    - The chain returns the original instance.
    - State reflects the configuration of the last call (`earliest`).
    - The `_recorded_desc` list still contains the field from the first call.
    """
    # Chain the two calls.
    result = agg_mixin.latest("uid", int, "ts").earliest("email", str, "updated_at")

    # The chain must return the original object.
    assert result is agg_mixin

    # State reflects the _earliest_ configuration (the last call wins).
    assert agg_mixin._unique_field == "email"
    assert agg_mixin._keyword_suffix == ".keyword"  # str → .keyword
    assert agg_mixin._recorded_asc == ["updated_at"]  # from earliest()
    assert agg_mixin.sort == [{"updated_at": {"order": "asc"}}]

    # The descending call made by latest() is still recorded.
    assert agg_mixin._recorded_desc == ["ts"]


def test_agg_mixin_chaining_latest_and_manual_descending(
    agg_mixin: DummyAggMixin,
) -> None:
    """Verify chaining latest() with an explicit descending() call.

    This test ensures that after calling `latest()` (which internally calls
    `descending`), an additional explicit call to `descending()` adds to the
    recorded history and updates the `sort` order to the new field. This confirms
    that manual sorting overrides the automatic sorting set by the aggregation helper.

    Assertions:
    - The chain returns the original instance.
    - `_recorded_desc` contains both the automatic and manual fields.
    - `sort` reflects the last manual call.
    - `_recorded_asc` remains empty.
    """
    # Chain a latest call with an explicit extra descending call.
    result = agg_mixin.latest("uid", int, "ts").descending("created_at")

    # The chain must be fluent.
    assert result is agg_mixin

    # latest recorded the first descending field.
    # The explicit call adds a second entry.
    assert agg_mixin._recorded_desc == ["ts", "created_at"]

    # sort reflects the _last_ descending call.
    assert agg_mixin.sort == [{"created_at": {"order": "desc"}}]

    # No ascending calls have been recorded.
    assert agg_mixin._recorded_asc == []
