# sync_helper.py
import functools
from collections.abc import Callable
from typing import Any, TypeVar

from opensearch_dsl import Q

# TypeVar bound to any object that has the required attributes (_must, _search, etc.)
T = TypeVar("T")


class DslSyncHelper:
    """Static helper class containing the logic to sync query clauses.

    This class provides methods to synchronize accumulated query clauses
    (_must, _filter, _must_not) from mixin instances into the underlying
    opensearch_dsl Search object, ensuring the final query reflects all
    fluent chaining operations.
    """

    @staticmethod
    def sync(target: Any) -> None:
        """Performs the synchronization logic on the target object.

        This method reconstructs the root query of the Search object by combining
        the accumulated _must, _must_not, and _filter lists into a single 'bool' query.

        Implementation Detail:
        We dynamically build the keyword arguments for the Q('bool', ...) constructor
        to include only non-empty lists. This is required because passing `None`
        explicitly to the constructor causes opensearch-dsl to attempt to resolve
        'None' as a query type name, resulting in an UnknownDslObject error.

        :param target: The mixin instance (self) containing _must, _filter, _search, etc.
        """
        # Only rebuild if we have accumulated clauses
        if target._must or target._must_not or target._filter:
            # Prepare kwargs dynamically to avoid passing None to the constructor
            bool_kwargs = {}

            if target._must:
                bool_kwargs["must"] = target._must

            if target._must_not:
                bool_kwargs["must_not"] = target._must_not

            if target._filter:
                bool_kwargs["filter"] = target._filter

            # Create the Bool query ONLY with non-empty lists
            q = Q("bool", **bool_kwargs)

            # Apply the bool query to the search object
            target._search = target._search.query(q)

        # Re-apply PIT if it exists
        if hasattr(target, "pit_id") and target.pit_id:
            target._search = target._search.extra(pit={"id": target.pit_id, "keep_alive": "5m"})

        # Ensure size is set
        if hasattr(target, "size"):
            target._search = target._search.params(size=target.size)

    @staticmethod
    def auto_sync(func: Callable[..., T]) -> Callable[..., T]:
        """Decorator that calls DslSyncHelper.sync(self) after the wrapped method executes.

        Use this on any fluent method that modifies query state (_must, _filter, _must_not,
        sort, etc.) to ensure the underlying opensearch_dsl.Search object remains consistent
        with the internal clause lists.

        :param func: The fluent method to wrap.
        :return: The wrapped method that syncs state upon completion.
        """

        @functools.wraps(func)
        def wrapper(self, *args, **kwargs) -> T:
            # Execute the original method (e.g., appending to _must)
            result = func(self, *args, **kwargs)
            # Sync the state
            DslSyncHelper.sync(self)
            return result

        return wrapper


# Module-level alias for easy exporting
auto_sync = DslSyncHelper.auto_sync
