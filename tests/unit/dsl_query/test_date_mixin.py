import datetime as _dt

from mockito import ANY, when

from tests.unit.conftest import DummyDateMixin


def test_on_or_after_adds_gte_clause(date_mixin: DummyDateMixin, _unstub):
    when(date_mixin)._to_iso(ANY).thenReturn("2023-01-01T00:00:00Z")  # stub conversion
    result = date_mixin.on_or_after("created_at", _dt.datetime(2023, 1, 1))

    # method should be fluent
    assert result is date_mixin

    # exactly one call to _add_range with the expected kwargs
    assert date_mixin._range_calls == [("created_at", {"gte": "2023-01-01T00:00:00Z"})]


def test_on_or_before_adds_lte_clause(date_mixin: DummyDateMixin, _unstub):
    when(date_mixin)._to_iso(ANY).thenReturn("2024-12-31T23:59:59Z")
    result = date_mixin.on_or_before("updated_at", "2024-12-31T23:59:59Z")
    assert result is date_mixin
    assert date_mixin._range_calls == [("updated_at", {"lte": "2024-12-31T23:59:59Z"})]


def test_after_adds_gt_clause(date_mixin: DummyDateMixin, _unstub):
    when(date_mixin)._to_iso(ANY).thenReturn("2022-06-15T12:00:00Z")
    result = date_mixin.after("event_ts", "2022-06-15T12:00:00Z")
    assert result is date_mixin
    assert date_mixin._range_calls == [("event_ts", {"gt": "2022-06-15T12:00:00Z"})]


def test_before_adds_lt_clause(date_mixin: DummyDateMixin, _unstub):
    when(date_mixin)._to_iso(ANY).thenReturn("2025-03-01T00:00:00Z")
    result = date_mixin.before("deadline", _dt.datetime(2025, 3, 1))
    assert result is date_mixin
    assert date_mixin._range_calls == [("deadline", {"lt": "2025-03-01T00:00:00Z"})]


def test_between_calls_on_or_after_and_on_or_before(date_mixin: DummyDateMixin, _unstub):
    # Stub the internal conversion to keep the test deterministic
    when(date_mixin)._to_iso(ANY).thenReturn("2021-01-01T00:00:00Z").thenReturn("2021-12-31T23:59:59Z")

    result = date_mixin.between(
        "publish_date",
        _dt.datetime(2021, 1, 1),
        _dt.datetime(2021, 12, 31, 23, 59, 59),
    )

    assert result is date_mixin
    # between should have produced two separate range calls
    assert date_mixin._range_calls == [
        ("publish_date", {"gte": "2021-01-01T00:00:00Z"}),
        ("publish_date", {"lte": "2021-12-31T23:59:59Z"}),
    ]


def test_chaining(date_mixin: DummyDateMixin, _unstub):
    """Demonstrates a fluent chain.

    The chain consists of:
        .on_or_after(...)
        .before(...)
        .greater_than(...)

    The mix-in must return self after each call and record all three range clauses
    in the order they were added.
    """
    # Stub the ISO conversion for the two date helpers.
    when(date_mixin)._to_iso(ANY).thenReturn("2023-01-01T00:00:00Z").thenReturn("2023-12-31T23:59:59Z")

    # fluent chain
    result = (
        date_mixin.on_or_after("created_at", "2023-01-01").before("deadline", "2023-12-31").greater_than("price", 10)
    )

    # The chain must return the original object.
    assert result is date_mixin

    # All three range calls should be present, in the order invoked.
    assert date_mixin._range_calls == [
        ("created_at", {"gte": "2023-01-01T00:00:00Z"}),
        ("deadline", {"lt": "2023-12-31T23:59:59Z"}),  # before uses lt
        ("price", {"gt": 10}),
    ]


def test_chaining_with_between_and_numeric(date_mixin: DummyDateMixin, _unstub):
    """Fluent-interface example that mixes a date range (via between) with a numeric filter."""
    # between calls on_or_after then on_or_before → two ISO conversions.
    when(date_mixin)._to_iso(ANY).thenReturn("2022-05-01T00:00:00Z").thenReturn("2022-05-31T23:59:59Z")

    result = date_mixin.between("sale_date", "2022-05-01", "2022-05-31").less_than_or_equal("quantity", 50)

    assert result is date_mixin
    assert date_mixin._range_calls == [
        ("sale_date", {"gte": "2022-05-01T00:00:00Z"}),
        ("sale_date", {"lte": "2022-05-31T23:59:59Z"}),
        ("quantity", {"lte": 50}),
    ]


def test_greater_than_numeric(date_mixin: DummyDateMixin, _unstub):
    result = date_mixin.greater_than("price", 10.5)
    assert result is date_mixin
    assert date_mixin._range_calls == [("price", {"gt": 10.5})]


def test_less_than_numeric(date_mixin: DummyDateMixin, _unstub):
    result = date_mixin.less_than("age", 30)
    assert result is date_mixin
    assert date_mixin._range_calls == [("age", {"lt": 30})]


def test_greater_than_or_equal_numeric(date_mixin: DummyDateMixin, _unstub):
    result = date_mixin.greater_than_or_equal("score", 0)
    assert result is date_mixin
    assert date_mixin._range_calls == [("score", {"gte": 0})]


def test_less_than_or_equal_numeric(date_mixin: DummyDateMixin, _unstub):
    result = date_mixin.less_than_or_equal("quantity", 100)
    assert result is date_mixin
    assert date_mixin._range_calls == [("quantity", {"lte": 100})]
