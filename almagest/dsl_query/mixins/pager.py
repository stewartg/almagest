from nested_lookup import nested_lookup
from opensearch_dsl import Index

from almagest.util.logging.simple_logger import SimpleLogger

from .base_mixin import BaseMixin


class PagerMixin(BaseMixin):
    """Opensearch helpers that use implement several tasks.

    Those are: 'search after' pagination, sample record retrieval, mappings, templates.
    """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._logger = SimpleLogger(self)

    def search_after(self, body: dict | None = None, timeout: int = 120) -> dict:
        """Query OpenSearch data using search-after pagination.

        Fetches all pages of results recursively. If `body` is None, it executes
        the query constructed via fluent chaining (e.g., exactly, after), relying
        on automatic state synchronization. If `body` is provided, it overrides
        all fluent filters and clears internal clause lists. Ensures a sort order
        is present for valid pagination.

        :param body: Optional raw DSL dictionary. If provided, overrides fluent filters.
        :param timeout: Request timeout in seconds.
        :return: List of source documents from all pages.
        """
        recs = []

        if body is None:
            if not self._search._sort:
                self._search = self._search.sort({"_id": "asc"})
        else:
            self._must.clear()
            self._must_not.clear()
            self._filter.clear()

            if not body.get("sort"):
                body["sort"] = [{"_id": "asc"}]

            self._search = self._search.update_from_dict(body)

        num_recs = self._page(recs, timeout, add_srch_after=False)

        while num_recs > 0:
            num_recs = self._page(recs, timeout)

        return nested_lookup("_source", recs)

    def _page(self, recs: list, timeout: int, add_srch_after: bool = True) -> int:
        """Fetch a single page of results and append the hits to 'recs'.

        The method optionally uses search-after so successive calls continue where
        the previous page left off.  When add_srch_after is True and 'recs' already
        contains hits, the sort value from the last hit is used to set search_after
        on the supplied Search object.

        :param srch: opensearch_dsl.Search instance
        :param recs: list of queried records
        :param timeout: equest timeout in seconds
        :param add_srch_after: default to true which continues the search-after
        :return: number of records retrieved or 0 if there was an error.
        """
        num_recs = 0
        if add_srch_after:
            last_sort = recs[-1].get("sort")
            if last_sort:
                self._search = self._search.extra(search_after=last_sort)
        try:
            page = []
            resp = self._search.params(request_timeout=timeout).execute()
            hits = resp.hits.hits
            for hit in hits:
                page.append(hit.to_dict())
            num_recs = len(page)
            if num_recs > 0:
                recs.extend(page)
        except Exception:
            num_recs = 0
            self._logger.exception(f"Search after failed for alias {self.index}.")
        return num_recs

    def get_sample_record(self) -> dict:
        """Retrieve a single sample record for the provided alias.

        :param alias: Index alias to query.
        :return: One hit dictionary (including '_source' and metadata).
        """
        rec = {}
        self._search.query("match_all")
        try:
            resp = self._search.params(size=1).execute()
            hits = resp.hits.hits
            rec = hits[0].to_dict()["_source"]
        except Exception:
            self._logger.exception(f"Sample record retrieval failed for {self.index}.")
        return rec

    def get_mappings(self) -> dict:
        """Retrieve the mapping definition for the index/alias held in self.index.

        The call is performed through the high-level opensearch_dsl.Index
        wrapper, which internally forwards the request to the underlying client.

        :return: mappings dict with same structure as opensearch.
        """
        # Index builds the proper URL (e.g. /my-index/_mapping) and
        # uses the same using=self._client that the mix-in already stores.
        idx = Index(self.index, using=self._client)

        # Index.get_mapping() returns a dict that mirrors the low-level
        # client.indices.get_mapping response.
        # If the index does not exist the underlying client raises
        # opensearchpy.exceptions.NotFoundError - we simply let it bubble
        # up so callers see the same error type as before.
        return idx.get_mapping()

    def get_template(self) -> dict:
        """Get the index template of the provided alias.

        :param alias: data alias
        :return: template definition for this alias.
        """
        return self._client.indices.get_template(name=self.index)
