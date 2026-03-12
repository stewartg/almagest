import datetime as _dt
from typing import Any, Optional, Union

# Since these are callables that return concrete classes, they can't be used in type hints
from opensearch_dsl import Q, Search

# Abstract base for all Q objects, used in hints
from opensearch_dsl.query import Query

from almagest.client_helper import ClientHelper
from almagest.dsl_query.dsl_sync_helper import DslSyncHelper


class BaseMixin:
    """Shared state and dynamic delegation logic for OpenSearch query builders.

    This class initializes the underlying OpenSearch client, search context, and
    query clause containers (must, must_not, filter). It also implements a custom
    **getattr** mechanism to enable seamless method delegation across mixin classes.

    How **getattr** works in this context:
    When an attribute (usually a method) is accessed that does not exist directly
    on the instance or in BaseMixin, Python invokes this method. It dynamically
    searches for the attribute in other mixin classes composed into the final
    concrete class. The search order is:
    1. Explicitly defined `_delegate_mixins` tuple (if set by the concrete class).
    2. The full Method Resolution Order (MRO) of the class, excluding BaseMixin
       and the base `object` class.

    Once found, if the attribute is callable, it is bound to the current instance
    `self`) before being returned. This allows methods defined in separate mixins
    (e.g., `MatchMixin.descending`) to be called as if they were native methods
    of the instance (e.g., `self.descending()`), maintaining access to the shared
    state initialized here.
    """

    # Concrete classes may set this to restrict the delegation order.
    # If not set, the full MRO (method resolution order) for delegates
    # is scanned, i.e. not BaseMixin.
    _delegate_mixins: tuple[type, ...] | None = None

    def __init__(
        self,
        index: str,
        *,
        size: int = 10000,
        sort: list[dict[str, str]] | None = None,
        pit_id: str | None = None,
        verify_certs: bool = False,
    ) -> None:
        """Initialize a BaseMixin instance.

        :param index: The OpenSearch index to query.
        :param size: Number of documents to retrieve per request.
        :param sort: list of sort specifications; defaults to
                             [{"_doc": "asc"}].
        :param search_after: search_after values for pagination, if any.
        :param pit_id: Point-in-time ID for consistent snapshots,
                             optional.
        :param *: Keyword-only argument separator. Ensures that all parameters following 'index'
            must be passed as keyword arguments (e.g., size=100) rather than positional arguments.
            This improves code readability and prevents errors if the parameter order changes
            in future updates.
        """
        self._client = ClientHelper().get_client(verify_certs=verify_certs)
        self.index = index
        self.size = size
        self.sort = sort or [{"_doc": "asc"}]
        # self.search_after = search_after
        self.pit_id = pit_id

        self._search: Search = Search(using=self._client, index=self.index).params(size=self.size)
        self._search = self._search.sort(*self.sort)

        self._must: list[Query] = []
        self._must_not: list[Query] = []
        self._filter: list[Query] = []
        self._unique_field: str | None = None
        self._keyword_suffix: str = ""
        self.after_key: dict[str, Any] | None = None

    @staticmethod
    def _to_iso(value: str | _dt.datetime) -> str:
        """Convert a datetime or ISO-8601 string to a UTC ISO-8601 string.

        :param value: datetime instance or ISO-8601 string.
        :return:      UTC ISO-8601 formatted string.
        """
        if isinstance(value, _dt.datetime):
            if value.tzinfo is None:
                value = value.replace(tzinfo=_dt.timezone.utc)
            else:
                value = value.astimezone(_dt.timezone.utc)
            return value.isoformat()
        return value

    def _add_range(self, field: str, **bounds: Any) -> None:
        """Append a range filter for the provided field.

        Here, Q returns a Query instance which is stored.

        :param field:   Name of the field to filter.
        :param bounds:  Keyword arguments describing the range
                        (e.g. gte=..., lt=...).
        """
        self._filter.append(Q("range", **{field: bounds}))

    def __getattr__(self, name: str) -> Any:
        """Resolve missing attributes by delegating to mixin classes.

        For example, the AggMixin calls self.descending() which is an attribute of the MatchMixin.
        Using the delgate mixins and the getattr strategy allow those to be resolved without
        using something like MatchMixin.descending().

        Search order:
        1. _delegate_mixins (if the concrete class defined it).
        2. All base-classes in the MRO except BaseMixin itself.

        The found attribute is rebound to self so that it operates on the
        same internal state.

        :param name: Attribute name being accessed.
        :return:     The attribute value (method bound to self or plain
                     attribute).
        :raises AttributeError: If the attribute cannot be found in any mixin.
        """
        mixins = getattr(self.__class__, "_delegate_mixins", None)
        if mixins is None:
            mixins = tuple(
                cls
                for cls in self.__class__.__mro__[1:]  # start after BaseMixin
                if cls is not object and hasattr(cls, "__dict__")
            )

        for mixin in mixins:
            if hasattr(mixin, name):
                attr = getattr(mixin, name)
                # Bind methods to this instance; plain attributes are returned as-is.
                return attr.__get__(self, self.__class__) if callable(attr) else attr

        raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{name}'")

    def reset(self) -> "BaseMixin":
        """Reset the query builder state to allow reuse of the client instance.

        This method clears all accumulated query clauses (_must, _filter, _must_not),
        resets aggregation state, and re-initializes the underlying _search object
        with the original default sort and size parameters.

        This allows the same FluentDslClient instance to be used for multiple
        independent queries without re-instantiation.

        :return: self (allows chaining immediately after reset).
        """
        # clear query clause lists
        self._must.clear()
        self._must_not.clear()
        self._filter.clear()

        # reset aggregation-specific state (from AggMixin)
        self._unique_field = None
        self._keyword_suffix = ""
        self.after_key = None

        self._search = Search(using=self._client, index=self.index).params(size=self.size)

        # re-apply the default sort
        default_sort = [{"_doc": "asc"}]
        self.sort = default_sort
        self._search = self._search.sort(*self.sort)

        return self
