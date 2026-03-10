from .mixins.agg import AggMixin
from .mixins.base_mixin import BaseMixin
from .mixins.date import DateMixin
from .mixins.match import MatchMixin
from .mixins.pager import PagerMixin
from .mixins.update import UpdateMixin


class FluentDslClient(MatchMixin, AggMixin, DateMixin, PagerMixin, UpdateMixin, BaseMixin):
    """Initialize the DslClient.

    Composes multiple functional mixins (Matching, Aggregation, Date Filtering,
    and Pagination) to provide a fluent interface for constructing and executing
    complex OpenSearch queries. It initializes the underlying search context,
    configures the target index, and sets up the shared state required by all
    mixin components. Although it is not necessary to include BaseMixin since all
    mixins call the BaseMixin init, it is included to indicate that BaseMixin is the
    base of the heirarchy.
    :param index: The name of the OpenSearch index to target.
    :param kwargs: Additional arguments passed to BaseMixin for client configuration
                   and logging setup.
    """

    def __init__(self, index: str, **kwargs):
        # Initialize the base state
        super().__init__(index=index, **kwargs)
