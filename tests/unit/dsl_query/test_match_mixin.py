from tests.unit.conftest import DummyMatchMixin


def test_descending_sets_sort_and_is_fluent(match_mixin: DummyMatchMixin, _unstub) -> None:
    """Verify descending() sets the sort clause and supports fluent chaining."""
    result = match_mixin.descending("price")
    assert result is match_mixin
    assert match_mixin.sort == [{"price": "desc"}]
    assert match_mixin._search.to_dict()["sort"] == [{"price": "desc"}]


def test_ascending_sets_sort_and_is_fluent(match_mixin: DummyMatchMixin, _unstub) -> None:
    """Verify ascending() sets the sort clause and supports fluent chaining."""
    result = match_mixin.ascending("created_at")
    assert result is match_mixin
    assert match_mixin.sort == [{"created_at": "asc"}]
    assert match_mixin._search.to_dict()["sort"] == [{"created_at": "asc"}]


def test_exists_adds_exists_filter_and_is_fluent(match_mixin: DummyMatchMixin, _unstub) -> None:
    """Verify exists() adds an exists filter clause and supports fluent chaining."""
    result = match_mixin.exists("user_id")
    assert result is match_mixin
    assert len(match_mixin._filter) == 1
    q = match_mixin._filter[0]
    assert q.to_dict() == {"exists": {"field": "user_id"}}


def test_does_not_exist_adds_must_not_and_is_fluent(match_mixin: DummyMatchMixin, _unstub) -> None:
    """Verify does_not_exist() adds a must_not clause and supports fluent chaining."""
    result = match_mixin.does_not_exist("deleted_at")
    assert result is match_mixin
    assert len(match_mixin._must_not) == 1
    q = match_mixin._must_not[0]
    assert q.to_dict() == {"exists": {"field": "deleted_at"}}


def test_exactly_int_uses_term_clause(match_mixin: DummyMatchMixin, _unstub) -> None:
    """Verify exactly() uses a term query for integer values."""
    result = match_mixin.exactly("age", 42)
    assert result is match_mixin
    assert len(match_mixin._must) == 1
    q = match_mixin._must[0]
    assert q.to_dict() == {"term": {"age": 42}}


def test_exactly_str_uses_term_clause(match_mixin: DummyMatchMixin, _unstub) -> None:
    """Verify exactly() uses a term query for string values (FIXED).

    Previously this used match_phrase. It now uses term for exact keyword matching.
    """
    result = match_mixin.exactly("status", "active")
    assert result is match_mixin
    assert len(match_mixin._must) == 1
    q = match_mixin._must[0]
    # EXPECTATION CHANGED: Now expects 'term' instead of 'match_phrase'
    assert q.to_dict() == {"term": {"status": "active"}}


def test_match_text_uses_match_phrase_clause(match_mixin: DummyMatchMixin, _unstub) -> None:
    """Verify match_text() uses a match_phrase query for full-text search (NEW)."""
    result = match_mixin.match_text("description", "critical error")
    assert result is match_mixin
    assert len(match_mixin._must) == 1
    q = match_mixin._must[0]
    assert q.to_dict() == {"match_phrase": {"description": "critical error"}}


def test_one_of_int_uses_terms_clause(match_mixin: DummyMatchMixin, _unstub) -> None:
    """Verify one_of() uses a terms query for lists of integers."""
    result = match_mixin.one_of("category_id", [1, 2, 3])
    assert result is match_mixin
    assert len(match_mixin._must) == 1
    q = match_mixin._must[0]
    assert q.to_dict() == {"terms": {"category_id": [1, 2, 3]}}


def test_one_of_str_uses_terms_clause(match_mixin: DummyMatchMixin, _unstub) -> None:
    """Verify one_of() uses a terms query for lists of strings (FIXED).

    Previously this used bool/should + match_phrase. It now uses terms for exact matching.
    """
    result = match_mixin.one_of("tag", ["red", "blue"])
    assert result is match_mixin
    assert len(match_mixin._must) == 1
    q = match_mixin._must[0]
    # EXPECTATION CHANGED: Now expects simple 'terms' instead of 'bool/should'
    assert q.to_dict() == {"terms": {"tag": ["red", "blue"]}}


def test_one_of_list_sets_doc_sort_and_terms_filter(match_mixin: DummyMatchMixin, _unstub) -> None:
    """Verify one_of_list() forces a terms filter and resets sort to _doc."""
    result = match_mixin.one_of_list("sku", ["A1", "B2", "C3"])
    assert result is match_mixin
    assert match_mixin.sort == [{"_doc": "asc"}]
    assert match_mixin._search.to_dict()["sort"] == [{"_doc": "asc"}]
    assert len(match_mixin._filter) == 1
    q = match_mixin._filter[0]
    assert q.to_dict() == {"terms": {"sku": ["A1", "B2", "C3"]}}


def test_one_exists_builds_bool_should_clause(match_mixin: DummyMatchMixin, _unstub) -> None:
    """Verify one_exists() builds a bool/should query for field existence."""
    result = match_mixin.one_exists(["a", "b", "c"])
    assert result is match_mixin
    assert len(match_mixin._must) == 1
    q = match_mixin._must[0]
    expected = {
        "bool": {
            "should": [
                {"exists": {"field": "a"}},
                {"exists": {"field": "b"}},
                {"exists": {"field": "c"}},
            ]
        }
    }
    assert q.to_dict() == expected


def test_chaining_all_helpers(match_mixin: DummyMatchMixin, _unstub) -> None:
    """Verify complex fluent chaining across all helper methods."""
    result = (
        match_mixin.descending("price")  # set sort -> price desc
        .exists("user_id")  # filter -> exists
        .does_not_exist("deleted_at")  # must_not -> exists
        .exactly("status", "active")  # must -> term (CHANGED)
        .one_of("category", [10, 20])  # must -> terms
        .one_of_list("sku", ["X", "Y"])  # filter -> terms + sort -> _doc asc
        .one_exists(["field1", "field2"])  # must -> bool/should
    )
    assert result is match_mixin

    # Sorting - one_of_list forces _doc asc
    assert match_mixin.sort == [{"_doc": "asc"}]
    assert match_mixin._search.to_dict()["sort"] == [{"_doc": "asc"}]

    # Filters
    assert len(match_mixin._filter) == 2
    assert match_mixin._filter[0].to_dict() == {"exists": {"field": "user_id"}}
    assert match_mixin._filter[1].to_dict() == {"terms": {"sku": ["X", "Y"]}}

    # must_not
    assert len(match_mixin._must_not) == 1
    assert match_mixin._must_not[0].to_dict() == {"exists": {"field": "deleted_at"}}

    # must - Updated expectations for term and terms
    assert len(match_mixin._must) == 3
    # 1. exactly() now uses 'term'
    assert match_mixin._must[0].to_dict() == {"term": {"status": "active"}}
    # 2. one_of() uses 'terms'
    assert match_mixin._must[1].to_dict() == {"terms": {"category": [10, 20]}}
    # 3. one_exists()
    expected_one_exists = {
        "bool": {
            "should": [
                {"exists": {"field": "field1"}},
                {"exists": {"field": "field2"}},
            ]
        }
    }
    assert match_mixin._must[2].to_dict() == expected_one_exists


def test_chaining_sort_exact_one_of_str(match_mixin: DummyMatchMixin, _unstub) -> None:
    """Verify chaining of sort, exact match, and multi-value string query."""
    result = match_mixin.ascending("timestamp").exactly("category", "books").one_of("tags", ["fiction", "bestseller"])
    assert result is match_mixin

    assert match_mixin.sort == [{"timestamp": "asc"}]
    assert match_mixin._search.to_dict()["sort"] == [{"timestamp": "asc"}]

    # exactly() now uses 'term'
    assert match_mixin._must[0].to_dict() == {"term": {"category": "books"}}

    # one_of() now uses 'terms' instead of bool/should
    assert match_mixin._must[1].to_dict() == {"terms": {"tags": ["fiction", "bestseller"]}}


def test_chaining_exists_must_not_one_exists(match_mixin: DummyMatchMixin, _unstub) -> None:
    """Verify chaining of existence checks across filter, must_not, and must."""
    result = match_mixin.exists("email").does_not_exist("phone").one_exists(["address", "city"])
    assert result is match_mixin

    assert len(match_mixin._filter) == 1
    assert match_mixin._filter[0].to_dict() == {"exists": {"field": "email"}}

    assert len(match_mixin._must_not) == 1
    assert match_mixin._must_not[0].to_dict() == {"exists": {"field": "phone"}}

    expected_one_exists = {
        "bool": {
            "should": [
                {"exists": {"field": "address"}},
                {"exists": {"field": "city"}},
            ]
        }
    }
    assert match_mixin._must[0].to_dict() == expected_one_exists
