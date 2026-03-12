from typing import Any, Optional, Union

from opensearch_dsl import A, Q, Search

from almagest.dsl_query.dsl_sync_helper import auto_sync
from almagest.util.logging.simple_logger import SimpleLogger

from .base_mixin import BaseMixin


class AggMixin(BaseMixin):
    """Aggregation helpers for retrieving latest/earliest documents per unique field.

    This mixin extends BaseMixin to support composite aggregations that efficiently
    fetch the most recent or oldest document for each unique value in a specified field.
    It manages state required for these aggregations (unique_field, keyword_suffix)
    and provides methods to construct the corresponding OpenSearch query bodies.

    How it integrates with BaseMixin:
    Since AggMixin inherits from BaseMixin, it relies on BaseMixin's __getattr__ to
    resolve methods like `descending()` and `ascending()` which are typically defined
    in other mixins (e.g., SortMixin). This allows a fluent interface where methods
    from different concerns (sorting, filtering, aggregation) can be chained together
    seamlessly on a single instance.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Initialize the AggMixin.

        :param *args: Positional arguments passed to BaseMixin.
        :param **kwargs: Keyword arguments passed to BaseMixin.
        """
        super().__init__(*args, **kwargs)
        self._logger = SimpleLogger(self)
        # State that is populated by ``latest`` / ``earliest``.
        # Re-declared here for type hint clarity in this context,
        # though initially set in BaseMixin to None.
        self._unique_field: str | None = None
        self._keyword_suffix: str = ""

    @auto_sync
    def latest(
        self,
        unique_field: str,
        field_type: type[int] | type[str],
        time_field: str,
    ) -> "AggMixin":
        """Prepare an aggregation that returns the most recent document per unique_field.

        Configures the internal state to perform a composite aggregation grouped by
        `unique_field`, sorting by `time_field` in descending order to capture the
        latest entry.

        :param unique_field: Field name that uniquely identifies a record.
        :param field_type: type of unique_field (int or str). Determines whether
                           a `.keyword` suffix is added (required for string aggregations).
        :param time_field: Name of the timestamp field used for sorting.
        :return: The AggMixin instance (allows method chaining).
        """
        self._unique_field = unique_field
        self._keyword_suffix = "" if field_type is int else ".keyword"
        self.descending(time_field)
        return self

    @auto_sync
    def earliest(
        self,
        unique_field: str,
        field_type: type[int] | type[str],
        time_field: str,
    ) -> "AggMixin":
        """Prepare an aggregation that returns the oldest document per unique_field.

        Configures the internal state to perform a composite aggregation grouped by
        `unique_field`, sorting by `time_field` in ascending order to capture the
        earliest entry.

        :param unique_field: Field name that uniquely identifies a record.
        :param field_type: type of unique_field (int or str). Determines whether
                           a `.keyword` suffix is added (required for string aggregations).
        :param time_field: Name of the timestamp field used for sorting.
        :return: The AggMixin instance (allows method chaining).
        """
        self._unique_field = unique_field
        self._keyword_suffix = "" if field_type is int else ".keyword"
        self.ascending(time_field)
        return self

    def to_dict(self) -> dict:
        """Return a JSON-compatible dict ready for client.search(body=…).

        :return: If an aggregation (latest/earliest) is configured, the
                 aggregate query dict is emitted; otherwise a normal search
                 query dict is returned.
        """
        if self.pit_id and self._unique_field:
            return self._build_aggregate_query()
        return self._build_standard_query()

    def _build_standard_query(self) -> dict:
        """Build the standard (non-aggregate) search query.

        :return: Dictionary representing the standard OpenSearch query.
        """
        return self._search.to_dict()

    def _build_aggregate_query(self) -> dict:
        """Build the aggregate query used by latest or earliest.

        :raises ValueError: If unique_field has not been set via
                            latest() or earliest().
        :return: Dictionary representing the aggregate OpenSearch query.
        """
        if not self._unique_field:
            raise ValueError("unique_field must be set via latest() or earliest()")

        base = Search(index=self.index).params(size=0)

        if self._must:
            base = base.query(Q("bool", must=self._must))
        if self._must_not:
            base = base.query(Q("bool", must_not=self._must_not))
        if self._filter:
            base = base.filter(*self._filter)
        if self.pit_id:
            base = base.extra(pit={"id": self.pit_id, "keep_alive": "5m"})

        composite_source = {self._unique_field: {"terms": {"field": f"{self._unique_field}{self._keyword_suffix}"}}}
        composite_agg = A(
            "composite",
            size=10000,
            sources=[composite_source],
            after=self.after_key if self.after_key else None,
        )
        top_hits_agg = A("top_hits", size=1, sort=self.sort)

        base.aggs.bucket("unique_ids", "composite", composite_agg.to_dict())
        base.aggs["unique_ids"].bucket("latest_doc", "top_hits", top_hits_agg.to_dict())

        return base.to_dict()
