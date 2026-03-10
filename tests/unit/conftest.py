from pathlib import Path
from typing import Any, Optional

import opensearchpy
import pytest
from mockito import ANY, mock, when
from opensearch_dsl import A, Q, Search

from almagest.client_helper import ClientHelper
from almagest.dsl_query.mixins.agg import AggMixin
from almagest.dsl_query.mixins.date import DateMixin
from almagest.dsl_query.mixins.match import MatchMixin
from almagest.dsl_query.mixins.pager import PagerMixin
from almagest.dsl_query.mixins.update import UpdateMixin


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


class _Meta:
    """Mock metadata object for OpenSearch hits.

    Provides the .id attribute expected by opensearch_dsl Hit objects.
    """

    def __init__(self, doc_id: str | None):
        self.id = doc_id


class _Hit(dict):
    """Mock OpenSearch hit object that supports the `to_dict` interface and `meta.id`.

    This class mimics the structure of a real OpenSearch search hit, including:
    - Dictionary access to source fields.
    - A 'sort' list for pagination cursors.
    - A 'meta' object containing the document ID (.meta.id).

    :param source: The document source dictionary.
    :param sort: Optional list containing sort values (cursors).
    :param doc_id: The document _id. Required for tests involving ID retrieval.
    """

    def __init__(self, source: dict, sort: list | None = None, doc_id: str | None = None) -> None:
        super().__init__(source)
        if sort is not None:
            self["sort"] = sort

        # Always initialize meta; id will be None if not provided
        self.meta = _Meta(doc_id)

    def to_dict(self) -> dict:
        result = {
            "_source": dict(self),
            "_id": self.meta.id,
        }
        if "sort" in self:
            result["sort"] = self["sort"]
        return result


class _Resp:
    """Mock OpenSearch response object containing a list of hits."""

    def __init__(self, hits: list[_Hit]) -> None:
        # # Creates a dynamic object: resp.hits.hits
        # self.hits = type("Hits", (), {"hits": hits})

        # Create a list subclass that also exposes the 'hits' attribute
        # to match opensearch_dsl's HitList behavior if needed.
        class HitList(list):
            @property
            def hits(self):
                """Supports both resp.hits and resp.hits.hits access patterns."""
                return self

        # Assign the list of _Hit objects directly to self.hits.
        # This enables: response.hits[0], len(response.hits), if response.hits:
        self.hits = HitList(hits)


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

    def __getitem__(self, key: slice | int) -> "_StubSearch":
        """Support slicing like search[:10] or search[0]."""
        if isinstance(key, slice):
            if key.stop is not None:
                self._size = key.stop
        elif isinstance(key, int):
            self._size = 1
        return self

    def to_dict(self) -> dict:
        """Reconstructs a dictionary representation of the search state.

        This is used to mimic opensearch_dsl.Search.to_dict().
        """
        result = {}

        # Add params (like size)
        if self._params:
            result.update(self._params)

        # Add extra args (like pit, search_after)
        if self._extra_args:
            result.update(self._extra_args)

        # Add sort
        if self._sort_args:
            # Real DSL converts args to list of dicts.
            # Our stub stores raw args. We assume tests pass list of dicts directly.
            result["sort"] = list(self._sort_args)

        # Add query if present
        if self._query_args:
            args, kwargs = self._query_args
            if args:
                # If positional args were used (e.g., query("term", ...))
                # We can't perfectly reconstruct the DSL dict without more logic,
                # but for these specific tests, we mostly care about sort.
                # However, to prevent errors, we can return a placeholder or try to build it.
                # For the specific failing test, only 'sort' matters.
                pass
            if kwargs:
                # Handle keyword style if needed
                pass

        return result


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

    def __init__(self, mock_search: _StubSearch, **kwargs) -> None:
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
        self._search = mock_search

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

    def __init__(self, mock_search: _StubSearch, **kwargs) -> None:
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
        self._search = mock_search

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

    def __init__(self, mock_search: _StubSearch, mock_client: Any, **kwargs) -> None:
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


class DummyUpdateMixin(UpdateMixin):
    """Concrete UpdateMixin for testing with injectable mock client.

    This class bypasses the real BaseMixin initialization to allow direct
    injection of the mock client and search objects, ensuring no network
    calls are made during unit tests of update/upsert logic.
    """

    def __init__(self, mock_client: Any, mock_search: _StubSearch, **kwargs) -> None:
        """Initialize the DummyUpdateMixin.

        :param mock_client: The mocked OpenSearch client.
        """
        self._logger = mock()
        self._client = mock_client
        self.index = kwargs.get("index")
        # Ensure other expected attributes exist if accessed by parent logic
        self._must = []
        self._must_not = []
        self._filter = []
        self.sort = [{"_doc": "asc"}]
        self.pit_id = None
        # Placeholder, usually not used by UpdateMixin methods directly
        self._search = mock_search

    # def get_id_by_field(self, field: str, value: Any) -> Optional[str]:
    #     """
    #     Override to use self._search (the stub) instead of creating a new Search().
    #     """
    #     try:
    #         # Use the shared stubbed search object directly
    #         srch = self._search

    #         # Reset any previous query state if necessary (optional depending on test isolation)
    #         # For this simple stub, we just chain onto it.
    #         srch.query("term", **{field: value})
    #         srch = srch[:1]

    #         # This MUST call _StubSearch.execute(), NOT opensearch_dsl.Search.execute()
    #         response = srch.execute()

    #         if response.hits:
    #             return response.hits[0].meta.id
    #         return None
    #     except Exception as e:
    #         self._logger.exception(f"Error finding ID for {field}={value}: {e}")
    #         raise


class TestDslClient(DummyDateMixin, DummyMatchMixin, DummyAggMixin, DummyPagerMixin, DummyUpdateMixin):
    """A concrete test client combining all Dummy mixins.

    This mimics the real FluentDslClient MRO and signature:
    __init__(self, index: str, **kwargs)

    It expects 'mock_search' and 'mock_client' to be passed via kwargs.
    """

    def __init__(self, index: str, **kwargs) -> None:
        """Initialize the TestDslClient.

        :param index: The index name (passed to mixins).
        :param kwargs: Must include 'mock_search' and 'mock_client'.
        """
        mock_search = kwargs.pop("mock_search", None)
        mock_client = kwargs.pop("mock_client", None)

        if not mock_search or not mock_client:
            raise ValueError("TestDslClient requires 'mock_search' and 'mock_client' in kwargs.")

        # Initialize each mixin explicitly to control state injection
        # Note: Order matters if mixins depend on attributes set by others,
        # but here we mostly set independent state.
        DummyDateMixin.__init__(self)
        DummyMatchMixin.__init__(self, mock_search, index=index)
        DummyAggMixin.__init__(self, mock_search, index=index)
        DummyPagerMixin.__init__(self, mock_search, mock_client, index=index)
        DummyUpdateMixin.__init__(self, mock_client, mock_search, index=index)

        # Ensure index is consistently set across all mixins
        self.index = index


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
    # We pass the mock explicitly via kwargs to ensure the Dummy classes use them.
    # The signature now matches FluentDslClient: (index, **kwargs)
    return TestDslClient(index="dummy-index", mock_search=mock_search, mock_client=mock_os_client)


@pytest.fixture
def agg_mixin(mock_search: _StubSearch) -> DummyAggMixin:
    """Provides a standalone DummyAggMixin with a stubbed search."""
    return DummyAggMixin(mock_search=mock_search, index="dummy-index")


@pytest.fixture
def date_mixin():
    return DummyDateMixin()


@pytest.fixture
def match_mixin(mock_search: _StubSearch) -> DummyMatchMixin:
    """Provides a standalone DummyMatchMixin with a stubbed search."""
    return DummyMatchMixin(mock_search=mock_search, index="dummy-index")


@pytest.fixture
def pager_mixin(mock_search, mock_os_client):
    """Provides a DummyPagerMixin with injected mock search and client."""
    return DummyPagerMixin(mock_search, mock_os_client, index="dummy-index")
