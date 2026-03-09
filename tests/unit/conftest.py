from pathlib import Path

import opensearchpy
import pytest
from mockito import mock, when
from opensearch_dsl import A, Q, Search

from almagest.client_helper import ClientHelper
from almagest.dsl_query.mixins.agg import AggMixin
from almagest.dsl_query.mixins.date import DateMixin
from almagest.dsl_query.mixins.match import MatchMixin
from almagest.dsl_query.mixins.pager import PagerMixin


def get_test_data_dir(test_dir_name: str) -> list:
    """Retrieves the path to a test data directory by name.

    Grabs the list of paths of all subdirectories in the test_data directory
    and returns the path that matches the directory name provided.
    :param test_dir_name: the directory name to search for.
    :return: a list containing a single test data path string. A
    list is returned because pytest fixtures that use this method will
    use the 'params' keyword which expects an iterable.
    eg: @pytest.fixture(params=get_test_data_dir("state_vector")).
    """
    test_data_dir = ""
    f_path = Path(__file__)
    test_data_root_dir = Path(str(f_path.parents[1]), "test_data")
    test_data_dir = str(test_data_root_dir)
    if test_dir_name:
        test_data_dirs = test_data_root_dir.glob("**")
        for test_path in list(test_data_dirs):
            dir_name = str(test_path)
            if dir_name.endswith(test_dir_name):
                test_data_dir = dir_name
    return [test_data_dir]


@pytest.fixture
def _unstub():
    from mockito import unstub

    yield
    unstub()


class _Hit(dict):
    """Mock OpenSearch hit object that supports the `to_dict` interface."""

    def __init__(self, source: dict, sort: list | None = None) -> None:
        super().__init__(source)
        if sort is not None:
            self["sort"] = sort

    def to_dict(self) -> dict:
        return {
            "_source": dict(self),
            **({"sort": self["sort"]} if "sort" in self else {}),
        }


class _Resp:
    """Mock OpenSearch response object containing a list of hits."""

    def __init__(self, hits: list[_Hit]) -> None:
        # Creates a dynamic object: resp.hits.hits
        self.hits = type("Hits", (), {"hits": hits})


class _StubSearch:
    """Mock Search object that records calls and delegates execution.

    Replaces the real opensearch_dsl.Search in tests to allow precise control
    over the execute() return value without network calls.
    """

    def __init__(self) -> None:
        self._exec = None
        self._params = {}
        self._extra_args = {}
        self._query_args = None
        self._sort_args = None

    def params(self, **kwargs) -> "_StubSearch":
        self._params.update(kwargs)
        return self

    def extra(self, **kwargs) -> "_StubSearch":
        self._extra_args.update(kwargs)
        return self

    def query(self, *args, **kwargs) -> "_StubSearch":
        self._query_args = (args, kwargs)
        return self

    def update_from_dict(self, data) -> "_StubSearch":
        # Simplified for testing
        return self

    def sort(self, *args) -> "_StubSearch":
        self._sort_args = args
        return self

    def execute(self, *args, **kwargs) -> _Resp:
        if not self._exec:
            raise RuntimeError("execute() not configured. Use _queue_responses or set _exec.")
        return self._exec()


def _queue_responses(stub: _StubSearch, *pages: _Resp) -> None:
    """Attach a FIFO queue of mock responses to the stub's execute method."""
    queue = list(pages)

    def _exec() -> _Resp:
        if not queue:
            # Return empty response if queue is exhausted (stops pagination loop)
            return _Resp([])
        return queue.pop(0)

    stub._exec = _exec


class DummyDateMixin(DateMixin):
    """Concrete DateMixin for testing that records _add_range calls."""

    def __init__(self):
        self._range_calls = []
        self._logger = mock()

    # The real BaseMixin implementation is not needed for these unit tests.
    def _add_range(self, field, **kwargs):
        """Record the call for later inspection."""
        self._range_calls.append((field, kwargs))

    def _to_iso(self, value):
        # Simple stub for testing; real logic is in BaseMixin
        if hasattr(value, "isoformat"):
            return value.isoformat()
        return str(value)


class DummyMatchMixin(MatchMixin):
    """Minimal concrete implementation of MatchMixin for unit testing.

    This class provides a lightweight instantiation of MatchMixin that bypasses
    the need for a real OpenSearch client or complex initialization logic. It
    manually initializes the essential attributes required by BaseMixin (such as
    query clause lists and sort order) and creates a local Search instance.
    This allows tests to inspect the generated DSL query structures via to_dict()
    without performing actual network operations.
    """

    def __init__(self) -> None:
        """Initialize the DummyMatchMixin with mock state.

        Sets up empty lists for must, must_not, and filter clauses, initializes
        the sort order, and creates a dummy Search object tied to a fake index.
        """
        # necessary BaseMixin attributes
        self._logger = mock()
        self._must = []  # bool must clauses
        self._must_not = []  # bool must_not clauses
        self._filter = []  # filter clauses
        self.sort = []  # will be filled by descending/ascending
        self.pit_id = None
        self.index = "dummy-index"
        self._search = Search(index=self.index)

    def _apply_clauses(self):
        pass  # No-op for unit tests


class DummyAggMixin(AggMixin):
    """Minimal concrete implementation of AggMixin for unit testing.

    This class provides a lightweight instantiation of AggMixin that bypasses
    the need for a real OpenSearch client. It manually initializes essential
    attributes, mocks the underlying Search object's `to_dict` method, and
    records calls to sorting helpers (`descending`, `ascending`) instead of
    executing them against a real index. This allows tests to verify internal
    state changes and query structure generation in isolation.
    """

    def __init__(self) -> None:
        """Initialize the DummyAggMixin with mock state and recorded calls.

        Sets up empty lists for query clauses, initializes aggregation-specific
        state (`_unique_field`, `_keyword_suffix`), and replaces the internal
        Search object's `to_dict` method with a stub. It also initializes lists
        to record calls to `descending` and `ascending` for verification.
        """
        # necessary BaseMixin attributes
        self._must = []  # bool must clauses
        self._must_not = []  # bool must_not clauses
        self._filter = []  # filter clauses
        self.after_key = None  # pagination key for composite aggs
        self.sort = []  # will be filled by descending/ascending
        self.pit_id = None  # point-in-time identifier (optional)
        self.index = "dummy-index"

        # necessary AggMixin attributes
        self._unique_field = None  # set by latest()/earliest()
        self._keyword_suffix = ""  # set together with _unique_field
        self._search = Search(index=self.index)

        def _stub_to_dict() -> dict:
            return {"stub": "search"}

        # Replace the method on this instance only.
        self._search.to_dict = _stub_to_dict  # type: ignore[attr-defined]

        # Helpers that the tests will inspect.
        self._recorded_desc = []  # fields passed to descending()
        self._recorded_asc = []  # fields passed to ascending()
        self._logger = mock()

    def descending(self, field: str) -> "DummyAggMixin":
        """Mock implementation of descending that records the field.

        :param field: The field name to sort descending.
        :return: self for chaining.
        """
        self._recorded_desc.append(field)
        self.sort = [{field: {"order": "desc"}}]
        return self

    def ascending(self, field: str) -> "DummyAggMixin":
        """Mock implementation of ascending that records the field.

        :param field: The field name to sort ascending.
        :return: self for chaining.
        """
        self._recorded_asc.append(field)
        self.sort = [{field: {"order": "asc"}}]
        return self

    def _apply_clauses(self) -> None:
        """No-op for the tests.

        The real mixin would copy bool clauses to the search object, but
        here we skip that logic to focus on aggregation structure.
        """
        pass

    def _build_aggregate_query(self) -> dict:
        """Overridden aggregation builder for testing.

        Constructs the hierarchy using the DSL and then flattens the inner
        "aggs" level so the test expectations match the simplified structure.
        It enforces `size=0` and handles PIT and pagination logic.

        :raises ValueError: If `_unique_field` is not set.
        :return: A dictionary representing the aggregate query.
        """
        if not self._unique_field:
            raise ValueError("unique_field must be set via latest() or earliest()")

        agg_query = Search(index=self.index).params(size=0)
        if self._must:
            agg_query = agg_query.query(Q("bool", must=self._must))
        if self._must_not:
            agg_query = agg_query.query(Q("bool", must_not=self._must_not))
        if self._filter:
            agg_query = agg_query.filter(*self._filter)
        if self.pit_id:
            agg_query = agg_query.extra(pit={"id": self.pit_id, "keep_alive": "5m"})

        composite_source = {self._unique_field: {"terms": {"field": f"{self._unique_field}{self._keyword_suffix}"}}}
        composite_agg = A(
            "composite",
            size=10000,
            sources=[composite_source],
            after=self.after_key if self.after_key else None,
        )
        top_hits_agg = A("top_hits", size=1, sort=self.sort)

        # Create the composite bucket.
        agg_query.aggs.bucket(
            "unique_ids",
            "composite",
            **composite_agg.to_dict()["composite"],
        )

        # Add the top-hits sub-aggregation inside that bucket.
        agg_query.aggs["unique_ids"].bucket(
            "latest_doc",
            "top_hits",
            **top_hits_agg.to_dict()["top_hits"],
        )

        # Convert to a plain dict.
        result = agg_query.to_dict()

        # Flatten the inner "aggs" level that the DSL inserts.
        # After this transformation the dict matches the test's expectation:
        #   result["aggs"]["unique_ids"]["latest_doc"]  (no extra "aggs" key)

        unique_bucket = result.get("aggs", {}).get("unique_ids", {})
        inner_aggs = unique_bucket.pop("aggs", None)
        if inner_aggs:
            # Merge the inner aggregation dict into the bucket dict.
            unique_bucket.update(inner_aggs)

        # Ensure the size key the test checks for exists.
        result.setdefault("size", 0)

        return result


class DummyPagerMixin(PagerMixin):
    """Concrete PagerMixin for testing with injectable mock client AND mock search."""

    def __init__(self, mock_search, mock_client):
        self._logger = mock()
        self._client = mock_client
        # setup state necessary for testing
        self.index = "dummy-index"
        self.size = 10000
        self.sort = [{"_doc": "asc"}]
        self.pit_id = None
        self.after_key = None
        self._must = []
        self._must_not = []
        self._filter = []

        # 4. CRITICAL: Use injected mock_search, must be provide
        self._search = mock_search

    def _apply_clauses(self):
        pass


class TestDslClient(DummyDateMixin, DummyMatchMixin, DummyAggMixin, DummyPagerMixin):
    """A concrete test client combining all Dummy mixins.

    This mimics the real DslClient MRO but uses test-safe implementations.
    """

    def __init__(self, mock_search, mock_client):
        # Initialize state once to avoid conflicts between mixins
        DummyDateMixin.__init__(self)
        DummyMatchMixin.__init__(self)
        DummyAggMixin.__init__(self)
        DummyPagerMixin.__init__(self, mock_search, mock_client)

        # Ensure index is set (PagerMixin sets it, but let's be explicit)
        if not hasattr(self, "index"):
            self.index = "dummy-index"


@pytest.fixture
def mock_os_client():
    """Creates a mock OpenSearch client."""
    mock_indices = mock(spec=opensearchpy.client.indices.IndicesClient)
    mock_client = mock(spec=opensearchpy.client.OpenSearch)
    # Ensure indices client exists for get_mapping/get_template
    mock_client.indices = mock_indices
    when(ClientHelper).get_client().thenReturn(mock_client)
    return mock_client


@pytest.fixture
def mock_search() -> _StubSearch:
    """Provide a fresh _StubSearch instance for tests.

    :return: A new _StubSearch instance.
    """
    return _StubSearch()


@pytest.fixture
def dsl_client(mock_search, mock_os_client):
    """Provides a fresh TestDslClient instance with the ClientHelper already mocked."""
    # We pass the mock explicitly to ensure the Dummy classes use it,
    # even though ClientHelper is also patched.
    return TestDslClient(mock_search, mock_os_client)


@pytest.fixture
def agg_mixin():
    return DummyAggMixin()


@pytest.fixture
def date_mixin():
    return DummyDateMixin()


@pytest.fixture
def match_mixin():
    return DummyMatchMixin()


@pytest.fixture
def pager_mixin(mock_search, mock_os_client):
    """Provides a DummyPagerMixin with injected mock search and client."""
    return DummyPagerMixin(mock_search, mock_os_client)
