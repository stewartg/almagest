import datetime as _dt

from almagest.util.logging.simple_logger import SimpleLogger

from .base_mixin import BaseMixin


class DateMixin(BaseMixin):
    """Date-time related helpers (range queries, between, etc.).

    All methods return 'self' for fluent chaining.
    """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._logger = SimpleLogger(self)

    def on_or_after(
        self,
        field: str,
        value: str | _dt.datetime,
    ) -> "DateMixin":
        """Add a >= (on or after) range clause.

        :param field: Name of the date field to query.
        :param value: ISO-8601 string or datetime object representing the
                      lower bound (inclusive).
        :return: 'self' (allows method chaining).
        """
        self._add_range(field, gte=self._to_iso(value))
        return self

    def on_or_before(
        self,
        field: str,
        value: str | _dt.datetime,
    ) -> "DateMixin":
        """Add a <= (on or before) range clause.

        :param field: Name of the date field to query.
        :param value: ISO-8601 string or datetime object representing the
                      upper bound (inclusive).
        :return: 'self' (allows method chaining).
        """
        self._add_range(field, lte=self._to_iso(value))
        return self

    def after(
        self,
        field: str,
        value: str | _dt.datetime,
    ) -> "DateMixin":
        """Add a > (strictly after) range clause.

        :param field: Name of the date field to query.
        :param value: ISO-8601 string or datetime object representing the
                      lower bound (exclusive).
        :return: 'self' (allows method chaining).
        """
        self._add_range(field, gt=self._to_iso(value))
        return self

    def before(
        self,
        field: str,
        value: str | _dt.datetime,
    ) -> "DateMixin":
        """Add a < (strictly before) range clause.

        :param field: Name of the date field to query.
        :param value: ISO-8601 string or datetime object representing the
                      upper bound (exclusive).
        :return: 'self' (allows method chaining).
        """
        self._add_range(field, lt=self._to_iso(value))
        return self

    def greater_than(
        self,
        field: str,
        value: int | float,
    ) -> "DateMixin":
        """Add a > numeric range clause.

        :param field: Name of the numeric field to query.
        :param value: Lower bound (exclusive).
        :return: 'self' (allows method chaining).
        """
        self._add_range(field, gt=value)
        return self

    def less_than(
        self,
        field: str,
        value: int | float,
    ) -> "DateMixin":
        """Add a < numeric range clause.

        :param field: Name of the numeric field to query.
        :param value: Upper bound (exclusive).
        :return: 'self' (allows method chaining).
        """
        self._add_range(field, lt=value)
        return self

    def greater_than_or_equal(
        self,
        field: str,
        value: int | float,
    ) -> "DateMixin":
        """Add a >= numeric range clause.

        :param field: Name of the numeric field to query.
        :param value: Lower bound (inclusive).
        :return: 'self' (allows method chaining).
        """
        self._add_range(field, gte=value)
        return self

    def less_than_or_equal(
        self,
        field: str,
        value: int | float,
    ) -> "DateMixin":
        """Add a <= numeric range clause.

        :param field: Name of the numeric field to query.
        :param value: Upper bound (inclusive).
        :return: 'self' (allows method chaining).
        """
        self._add_range(field, lte=value)
        return self

    def between(
        self,
        field: str,
        start: str | _dt.datetime,
        end: str | _dt.datetime,
    ) -> "DateMixin":
        """Add a closed-interval [start, end] range clause.

        :param field: Name of the date field to query.
        :param start: Lower bound (inclusive) - ISO-8601 string or datetime.
        :param end:   Upper bound (inclusive) - ISO-8601 string or datetime.
        :return: 'self' (allows method chaining).
        """
        self.on_or_after(field, start)
        self.on_or_before(field, end)
        return self
