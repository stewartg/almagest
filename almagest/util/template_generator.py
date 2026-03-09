from typing import Any

from opensearchpy.helpers import bulk

from almagest.client_helper import ClientHelper
from almagest.data_normalizer import DataNormalizer
from almagest.util.logging.simple_logger import SimpleLogger


class TemplateGenerator:
    """Dynamic OpenSearch index template generator based on sample data.

    This class automates the creation of index templates by analyzing a sample
    dataset. It normalizes the data, performs a temporary bulk insert to let
    OpenSearch infer mappings, retrieves those mappings, and then constructs a
    reusable template associated with a specific alias. The temporary data and
    indices are cleaned up after the template is created.

    Workflow Overview:
    1. Validate input arguments (alias, date format, standard params).
    2. Normalize the provided sample data using DataNormalizer.
    3. Bulk index the normalized data into a temporary index to trigger mapping inference.
    4. Extract the generated mappings from the temporary index.
    5. Construct a template body with specific settings (shards, replicas) and the extracted mappings.
    6. Delete the temporary index and register the new template under the provided alias.
    """

    def __init__(self, verify_certs: bool = False) -> None:
        """Initialize the TemplateGenerator.

        Sets up the OpenSearch client, logger, and an empty dictionary for
        storing runtime arguments.
        :param verify_certs: True to verify certs.
        """
        self.logger = SimpleLogger(self)
        self.client = ClientHelper.get_client(verify_certs=verify_certs)
        self.args: dict[str, Any] = {}

    def _verify_args(self) -> None:
        """Verify that all required arguments for template generation are present.

        Checks the internal `self.args` dictionary for the presence of critical
        keys: `alias`, `index_partition_date_format`, and `standard_params`.
        If any are missing or invalid, a ValueError is raised to prevent
        downstream failures during index creation or normalization.

        :raises ValueError: If the 'alias' key is missing from index_details.
        :raises ValueError: If the 'index_partition_date_format' key is missing.
        :raises ValueError: If the 'standard_params' key is missing.
        """
        index_details = self.args.get("index_details")
        if not index_details:
            raise ValueError("The export args must contain a valid 'index_details' dictionary.")

        alias_name = index_details.get("alias")
        if not alias_name:
            raise ValueError('The export args must contain a valid dictionary containing the "alias" key.')

        index_partition_date_format = index_details.get("index_partition_date_format")
        if not index_partition_date_format:
            raise ValueError(
                'The export args must contain a valid dictionary containing the "index_partition_date_format" key.'
            )

        standard_params = self.args.get("standard_params")
        if not standard_params:
            raise ValueError('The export args must contain a valid dictionary containing the "standard_params" key.')

    def generate_template(self, data: list[dict[str, Any]], **kwargs: Any) -> None:
        """Generate an OpenSearch index template from sample data.

        This method orchestrates the full template creation lifecycle. It first
        ensures no template already exists for the given alias to avoid overwriting.
        It then normalizes the input data, bulk indexes it to a temporary index
        to allow OpenSearch to dynamically infer field mappings, and retrieves
        these mappings. Finally, it constructs a formal template with standardized
        settings and the inferred mappings, deletes the temporary data, and registers
        the template.

        Usage Pattern:
        1. Prepare your data in its final adjusted state.
        2. Create an exporter and configure necessary arguments.
        3. Use the exporter's `modify_data` method if transformation is needed.
        4. Pass the resulting data and configuration kwargs to this method.

        :param data: A list of dictionaries representing the sample data to be
                     analyzed. This data should be in its final, adjusted state
                     ready for indexing.
        :param **kwargs: Configuration arguments including:
                         - `index_details`: Dict containing 'alias' and 'index_partition_date_format'.
                         - `standard_params`: Dict containing parameters for the DataNormalizer.
        :raises ValueError: If required arguments are missing or if a template
                            already exists for the provided alias.
        """
        self.args = kwargs
        self._verify_args()

        alias_name = kwargs.get("index_details", {}).get("alias")
        if not alias_name:
            raise ValueError("Alias name could not be retrieved from arguments.")

        if self.client.indices.exists_template(name=alias_name):
            self.logger.warning(f"Template '{alias_name}' already exists. Skipping generation.")
            return

        processed_data = self._normalize_data(data)
        if not processed_data:
            raise ValueError("Processed data is empty. Cannot generate template from empty dataset.")

        # Bulk index to trigger mapping inference
        bulk(self.client, processed_data)

        # Retrieve the actual index name created by the normalizer
        index_name = processed_data[0]["_index"]
        index_metadata = self.client.indices.get(index=index_name)
        mappings = index_metadata[index_name]["mappings"]

        template_body = {
            "template": f"{alias_name}",
            "index_patterns": [f"{alias_name}_*"],
            "settings": {
                "index": {
                    "number_of_shards": "1",
                    "number_of_replicas": "2",
                }
            },
            "mappings": mappings,
            "aliases": {f"{alias_name}": {}},
        }

        # Cleanup temporary index
        self.client.indices.delete(index=index_name)

        # Create the template
        self.client.indices.put_template(name=alias_name, body=template_body)
        self.logger.info(f"Template '{alias_name}' successfully created.")

    def _normalize_data(self, modified_data: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Normalize and prepare data for bulk ingestion into OpenSearch.

        Utilizes the `DataNormalizer` to standardize the input data based on
        the configured `index_details`. This includes handling index suffixes
        based on date formats and converting records into the format required
        by the OpenSearch bulk API.

        :param modified_data: The list of dictionaries (records) to be normalized
                              and prepared for writing.
        :return: A list of dictionaries formatted specifically for the OpenSearch
                 bulk API, including action/metadata lines if handled internally
                 by the normalizer, or just source documents depending on implementation.
        :raises ValueError: If index creation fails during the normalization verification step.
        """
        idx_args = self.args.get("index_details")
        if not idx_args:
            raise ValueError("Missing 'index_details' in arguments for normalization.")

        normalizer = DataNormalizer(modified_data, **idx_args)
        args = self.args.get("standard_params")
        if not args:
            raise ValueError("Missing 'standard_params' in arguments for normalization.")

        bulk_data = normalizer.standardize_and_bulkify(**args)
        self._verify_indices(normalizer.indices)
        return bulk_data

    def _verify_indices(self, indices: list[str]) -> None:
        """Ensure all required indices exist in OpenSearch.

        Iterates through the list of indices generated by the `DataNormalizer`.
        For each index, it checks for existence. If an index does not exist,
        it attempts to create it. This ensures that the bulk operation in
        `generate_template` will not fail due to missing indices.

        :param indices: A list of index names that must exist in OpenSearch.
        :raises ValueError: If the creation of any index fails, including the
                            specific error message from the client.
        """
        for idx in indices:
            try:
                if not self.client.indices.exists(index=idx):
                    self.client.indices.create(index=idx)
                    self.logger.debug(f"Created index: {idx}")
            except Exception as err:
                raise ValueError(f"Creating index: {idx} failed because {err}") from err
