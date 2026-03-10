from typing import Any, Optional

from opensearch_dsl import Search
from opensearchpy import NotFoundError

from almagest.util.logging.simple_logger import SimpleLogger

from .base_mixin import BaseMixin


class UpdateMixin(BaseMixin):
    """Helpers for updating, upserting, and fetching documents by ID or unique field.

    This mixin integrates with the existing fluent interface by inheriting from BaseMixin.
    It combines opensearch_dsl for read/lookup operations (consistency) and the low-level
    client for write operations (flexibility with dynamic dictionaries).
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Initialize the UpdateMixin.

        :param *args: Positional arguments passed to BaseMixin.
        :param **kwargs: Keyword arguments passed to BaseMixin.
        """
        super().__init__(*args, **kwargs)
        self._logger = SimpleLogger(self)

    def get_by_id(self, doc_id: str) -> Optional[dict[str, Any]]:
        """Fetch a single document directly by its OpenSearch _id.

        Uses the efficient 'get' API via the underlying client.

        :param doc_id: The OpenSearch _id.
        :return: The document source dict, or None if not found.
        """
        found_doc = None
        try:
            response = self._client.get(index=self.index, id=doc_id)
            found_doc = response.get("_source")
        except NotFoundError:
            self._logger.debug(f"Document {doc_id} not found in {self.index}")
        except Exception:
            self._logger.exception(f"Error fetching document {doc_id}.")
            raise
        return found_doc

    def get_id_by_field(self, field: str, value: Any) -> Optional[str]:
        """Find the OpenSearch _id by querying a unique field.

        Useful when you know a business key (e.g., email, user_id) but not the _id.
        Uses opensearch_dsl.Search for consistency with other mixins.

        :param field: The field name to query (should be unique/keyword).
        :param value: The value to match.
        :return: The OpenSearch _id, or None if not found.
        """
        found_id = None
        try:
            srch = Search(index=self.index, using=self._client)
            srch = srch.query("term", **{field: value})
            srch = srch[:1]  # Only need one result

            response = srch.execute()

            if response.hits:
                found_id = response.hits[0].meta.id
        except Exception:
            self._logger.exception(f"Error finding ID for {field}={value}")
            raise
        return found_id

    def update_record(
        self,
        doc_id: str,
        body: dict[str, Any],
        refresh: bool = False,
        retry_on_conflict: int = 3,
    ) -> dict[str, Any]:
        """Perform a partial update on an existing document.

        :param doc_id: The  _id.
        :param body: Dictionary of fields to update (merged into existing doc).
        :param refresh: If True, refresh the index immediately after update.
        :param retry_on_conflict: Number of times to retry on version conflicts.
        :return: The raw response from the update API.
        :raises NotFoundError: If the document does not exist.
        """
        update_body = {"doc": body}
        params = {
            "refresh": "true" if refresh else "false",
            "retry_on_conflict": retry_on_conflict,
        }
        self._logger.debug(f"Updating document {doc_id} in {self.index}")
        return self._client.update(
            index=self.index,
            id=doc_id,
            body=update_body,
            params=params,  # Pass options here
        )

    def upsert_record(
        self,
        doc_id: str,
        body: dict[str, Any],
        default_body: Optional[dict[str, Any]] = None,
        refresh: bool = False,
    ) -> dict[str, Any]:
        """Update a document or insert it if it doesn't exist.

        :param doc_id: The OpenSearch _id.
        :param body: Dictionary of fields to update/insert.
        :param default_body: Optional initial content if the document is created.
                             If None, 'body' is used as the initial content.
        :param refresh: If True, refresh the index immediately.
        :return: The raw response from the update API.
        """
        update_body: dict[str, Any] = {"doc": body, "doc_as_upsert": True}

        if default_body is not None:
            update_body["upsert"] = default_body

        self._logger.debug(f"Upserting document {doc_id} in {self.index}")
        params = {"refresh": "true" if refresh else "false"}
        return self._client.update(
            index=self.index,
            id=doc_id,
            body=update_body,
            params=params,
        )
