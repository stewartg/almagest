import time
from abc import ABCMeta, abstractmethod

from opensearchpy.helpers import bulk

from almagest.client_helper import ClientHelper
from almagest.data_normalizer import DataNormalizer
from almagest.util.logging.simple_logger import SimpleLogger


class AbstractDataExporter(metaclass=ABCMeta):
    """Prepare sdata to be written to the indicated opensearch cluster."""

    def __init__(self, verify_certs: bool = False) -> None:
        self.class_name = self.__class__.__name__
        self.logger = SimpleLogger(self)
        self.client = ClientHelper.get_client(verify_certs=verify_certs)
        self._batch_size = 1000
        self._throttle_time = 0
        self._args = {}

    def export(self, data: list[dict], **kwargs) -> tuple[int, int]:
        """Exports data to various endpoints.

        Can can be throttled by setting the batch_size and throttle_time parameters.
        This method will:
        1. Verify that the proper arguments are provided for each exporter type and raise
           an exception if any are missing
        2. Calls the modify_data method which is provided for any data manipulation that
           should happed before the data is exported.
        3. Calls the export_data method which will export data to a corresponding endpoint
        :param data: a list of dictionaries that contain data to be exported
        :param kwargs: provided so that various exporters can provide any neccessary
        parameters. The concrete method verify_args should make sure the responsible
        exporter has the correct set of kwargs.
        :return: a tuple with the count of successful and failed exports.
        """
        self.args = kwargs
        self._verify_args()
        successes, failures = -1, -1
        processed_data = self._normalize_data(data)
        if processed_data:
            if self._batch_size > 0:
                chunks = [
                    processed_data[x : x + self._batch_size] for x in range(0, len(processed_data), self._batch_size)
                ]
                successes, failures = 0, 0
                for _i, chunk in enumerate(chunks):
                    try:
                        self._export_data(chunk)
                        successes += len(chunk)
                        self.logger.info(f"{self.class_name} exporter exported {len(chunk)} records.")
                        if self._throttle_time > 0:
                            time.sleep(self._throttle_time)
                    except Exception:
                        self.logger.exception("Exporter failed to export data because.")
                        failures += len(chunk)
            else:
                try:
                    self._export_data(processed_data)
                    successes += len(processed_data)
                    self.logger.info(f"{self.class_name} exporter exported {len(processed_data)} records.")
                except Exception:
                    self.logger.exception(f"{self.class_name} exporter failed to export data.")
                    failures += len(processed_data)
        return successes, failures

    def _verify_args(self):
        """Verifies that required args for each exporter type are provided.

        :raises ValueError: if any of the required arguments for the corresponding exporter type
        are missing or if a template doesn't exist for the provided alias.
        """
        alias_name = self.args.get("index_details").get("alias")
        if not alias_name:
            raise ValueError('The export args must contain a valid dictionary containing the "alias" key.')
        if not self.client.indices.exists_template(name=alias_name) and not self.client.indices.exists_index_template(
            alias_name
        ):
            raise ValueError(f"A template could not be found for alias: {alias_name}")
        index_partition_date_format = self.args.get("index_details").get("index_partition_date_format")
        if not index_partition_date_format:
            raise ValueError(
                'The export args must contain a valid dictionary containing the "index_partition_date_format" key.'
            )
        standard_params = self.args.get("standard_params")
        if not standard_params:
            raise ValueError('The export args must contain a valid dictionary containing the "standard_params" key.')

    def _normalize_data(self, data: list[dict]) -> list[dict]:
        """Standardizes/normalizes the provided data.

        Also prepares the data to be written to opensearch using the bulk api.
        :param data: the list of dictionaries to be written to opensearch
        :return: the altered data.
        """
        modified_data = self.modify_data(data)
        idx_args = self.args.get("index_details")
        normalizer = DataNormalizer(modified_data, **idx_args)
        args = self.args.get("standard_params")
        bulk_data = normalizer.standardize_and_bulkify(**args)
        self._verify_indices(normalizer.indices)
        return bulk_data

    @abstractmethod
    def modify_data(self, data: list[dict]) -> list[dict]:
        pass

    def _verify_indices(self, indices: list):
        """Makes sure that the indices exist.

        Ensures that indices that were created in the data_normalizer:_transform_index_suffix
        method exist in opensearch.
        :param indices: the list of indices to check
        :raises ValueError: if any of the indices cannot be created.
        """
        for idx in indices:
            try:
                if not self.client.indices.exists(index=idx):
                    self.client.indices.create(index=idx)
            except Exception as err:
                raise ValueError(f"Creating index: {idx} failed.") from err

    def _export_data(self, data: list[dict], **kwargs):
        """Calls the bulk api to write the data to opensearch.

        :param data: the data to write.
        """
        bulk(self.client, data)

    @property
    def batch_size(self):
        return self._batch_size

    @batch_size.setter
    def batch_size(self, value):
        self._batch_size = value

    @property
    def throttle_time(self):
        return self._throttle_time

    @throttle_time.setter
    def throttle_time(self, value):
        self._throttle_time = value

    @property
    def args(self):
        return self._args

    @args.setter
    def args(self, value):
        self._args = value
