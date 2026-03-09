from opensearch_dsl import Q

from almagest.util.logging.simple_logger import SimpleLogger

from .base_mixin import BaseMixin


class MatchMixin(BaseMixin):
    """Match related helpers.

    These helpers deal with exact matches, existence checks and simple
    multi-value queries.  All methods return 'self' for fluent chaining.
    """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._logger = SimpleLogger(self)

    # ------------------------------------------------------------------ #
    # Sorting
    # ------------------------------------------------------------------ #
    def descending(self, field: str) -> "MatchMixin":
        """Sort results by field in descending order.

        :param field: Name of the field to sort on.
        :return: 'self' (allows method chaining).
        """
        self.sort = [{field: "desc"}]
        self._search = self._search.sort(*self.sort)
        return self

    def ascending(self, field: str) -> "MatchMixin":
        """Sort results by field in ascending order.

        :param field: Name of the field to sort on.
        :return: 'self' (allows method chaining).
        """
        self.sort = [{field: "asc"}]
        self._search = self._search.sort(*self.sort)
        return self

    # ------------------------------------------------------------------ #
    # Existence
    # ------------------------------------------------------------------ #
    def exists(self, field: str) -> "MatchMixin":
        """Require that field exists in the document.

        :param field: Name of the field that must be present.
        :return: 'self' (allows method chaining).
        """
        self._filter.append(Q("exists", field=field))
        return self

    def does_not_exist(self, field: str) -> "MatchMixin":
        """Require that field does not exist in the document.

        :param field: Name of the field that must be absent.
        :return: 'self' (allows method chaining).
        """
        self._must_not.append(Q("exists", field=field))
        return self

    # ------------------------------------------------------------------ #
    # Exact/text match
    # ------------------------------------------------------------------ #
    def exactly(self, field: str, value: int | str | bool) -> "MatchMixin":
        """Match field exactly to value using a 'term' query.

        Use this for IDs, enums, statuses, booleans, and any field where
        you need a binary, byte-for-byte match.

        :param field: Name of the field to query.
        :param value: Exact value to match.
        :return: 'self' (allows method chaining).
        """
        self._must.append(Q("term", **{field: value}))
        return self

    def match_text(self, field: str, value: str) -> "MatchMixin":
        """Match field using 'match_phrase' for full-text search.

        Use this for searching within sentences, descriptions, or logs
        where tokenization, stemming, and analyzers are required.

        :param field: Name of the field to query.
        :param value: The text phrase to search for.
        :return: 'self' (allows method chaining).
        """
        self._must.append(Q("match_phrase", **{field: value}))
        return self

    # ------------------------------------------------------------------ #
    # Multi-value
    # ------------------------------------------------------------------ #
    def one_of(self, field: str, values: list[int | str]) -> "MatchMixin":
        """Match field to any value in the provided list.

        Logic:
        - If values are integers -> Uses 'terms' (exact numeric match).
        - If values are strings -> Uses 'terms' (exact string match on keywords).
        NOTE: We assume string filters are for keywords/IDs.
        For full-text search across multiple phrases, use a custom bool/should loop.

        :param field: Name of the field to query.
        :param values: List of values (all int or all str).
        :return: 'self' (allows method chaining).
        """
        if not values:
            return self

        # Check the type of the first element to determine strategy
        first_val = values[0]

        if isinstance(first_val, int):
            # Exact Numeric Match
            self._must.append(Q("terms", **{field: values}))

        elif isinstance(first_val, str):
            # Exact String Match
            # Uses 'terms' which works perfectly on 'keyword' fields for IDs/Enums.
            self._must.append(Q("terms", **{field: values}))

        else:
            # Fallback for booleans or mixed types
            self._must.append(Q("terms", **{field: values}))

        return self

    def one_of_list(self, field: str, values: list[int | str]) -> "MatchMixin":
        """Force a 'terms' filter while keeping the default '_doc' sort.

        Useful when an aggregation must respect the exact list order.
        :param field: Name of the field to filter on.
        :param values: List of values for the terms filter.
        :return: 'self' (allows method chaining).
        """
        self.sort = [{"_doc": "asc"}]
        self._search = self._search.sort(*self.sort)
        self._filter.append(Q("terms", **{field: values}))
        return self

    def one_exists(self, fields: list[str]) -> "MatchMixin":
        """Require that any of the supplied fields exist.

        :param fields: List of field names; at least one must be present.
        :return: 'self' (allows method chaining).
        """
        shoulds = [Q("exists", field=f) for f in fields]
        self._must.append(Q("bool", should=shoulds))
        return self
