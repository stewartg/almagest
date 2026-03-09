import hashlib
from datetime import datetime
from functools import wraps
from typing import Any

import numpy as np
import pandas as pd
from dateutil.parser import parse

# ruff: noqa: N806


class DataNormalizer:
    def __init__(self, data: list[dict], **kwargs) -> None:
        """Applies some basic data standardization/normalization.

        It is intended to be used for all data that will be inserted into an opensearch instance.
        :param data: the data to be standardized
        :param alias_name: the name of the opensearch alias for this data source
        :param index_partition_date_format: one of ['none', 'timeless', 'yearly', 'monthly', 'weekly'].
        """
        self.records = data
        self.alias = kwargs.get("alias")
        self.index_partition_date_format = kwargs.get("index_partition_date_format")
        self.index_version = kwargs.get("index_version", 1)
        self._indices: list[str] = []
        self.date_field = ""
        # List of regular expressions to replace in data. These will replace:
        # 1. Fields that contain only dashes
        # 2. Fields that contain only spaces
        # 3. Fields that contain only plus signs
        self.re_replace_list = [r"^-*$", r"^\s*$", r"^\+*$"]

    def standard_params(*standard_params: Any):
        """Check that all the keys identified to be standardized/normalized exist.

        :raises ValueError: if any of the keys are missing.
        """

        def _wrapper(func: Any) -> Any:
            @wraps(func)
            def _wrapped(*args: Any, **kwargs: Any) -> Any:
                for p in standard_params:
                    if p not in kwargs:
                        raise ValueError(f"Missing standard param: {p}")
                return func(*args, **kwargs)

            return _wrapped

        return _wrapper

    @standard_params(
        "classification_field",
        "start_date_field",
        "provider_details",
        "unique_fields",
    )
    def standardize_and_bulkify(self, **kwargs):
        """Helper that calls standardize, transform_index_suffix and bulkify.

        The methods are called in sequence and return the data in a format that can be ingested
        into opensearch. See the standardize and bulkify methods for more information.
        :return: the list of dictionaries.
        """
        data_df = self.standardize(**kwargs)
        data_map = self._transform_index_suffix(data_df)
        bulk_data = []
        for index, records in data_map.items():
            bulk_data.extend(self._bulkify(records, index, **kwargs))
        return bulk_data

    @standard_params(
        "classification_field",
        "start_date_field",
        "provider_details",
        "unique_fields",
    )
    def standardize(self, **kwargs):
        """Performs standardization/normalization based on the list of standard parameters.

        This method should be passed a dictionary whose keys are all of the standard parameters
        that have been identified and then calls methods that do standardization for
        each parameter.
        :return: the standardized data in a format that can be used with the opensearchpy.helpers
        bulk interface.
        """
        records_df = pd.DataFrame(self.records)
        # Replace nan variants and none values with empty string.
        to_replace = ["nan", "NAN", "NaN", np.nan, None] + self.re_replace_list
        records_df = records_df.replace(to_replace, "", regex=True)
        self._verify_classification(records_df, **kwargs)
        self._verify_date(records_df, **kwargs)
        self._add_provider_details(records_df, **kwargs)
        records_df["ingestTimestamp"] = records_df.apply(lambda x: pd.Timestamp.now(), axis=1)
        return records_df

    @standard_params("classification_field")
    def _verify_classification(self, data_df: pd.DataFrame, **kwargs):
        """Makes sure that the data contains the 'classification' field.

        If the data has classification data but the field is not called 'classification',
        pass the field name to be renamed 'classification' in the classification_field param,
        ie {'classification_field': 'classificationmarking}
        :param data_df: the data to check for classification
        :raises ValueError: if there is no 'classification' field and no classification field has been
        identified.
        """
        classification_field = kwargs.get("classification_field")
        df_cols = data_df.columns
        if "classification" not in df_cols:
            if not classification_field:
                raise ValueError("Your data must contain a classification field with a valid classification.")
            else:
                data_df.rename(columns={classification_field: "classification"}, inplace=True)

    @standard_params("start_date_field")
    def _verify_date(self, data_df: pd.DataFrame, **kwargs):
        """Maps a provided date field to the 'startTimestamp' field.

        This is optional since not all data has time. It will also add several date fields that can
        be useful for opensearch visualization that require date/time fields.
        :param data_df: the data to check to remap the date field.
        """
        start_date_field = kwargs.get("start_date_field")
        self.date_field = start_date_field
        if start_date_field:
            data_df[["monthOfYear_zulu", "dayOfWeek_zulu", "hourOfDay_zulu"]] = data_df[start_date_field].apply(
                lambda dt: self.parse_date(dt) if dt else dt
            )
            data_df.rename(columns={start_date_field: "startTimestamp"}, inplace=True)
            self.date_field = "startTimestamp"

    def parse_date(self, date_obj) -> pd.Series:
        """Parses a date into several components which are added to the data.

        :param date_obj: the date object
        :return: a pandas series that contains the date components or.
        """
        day_of_week = -1
        hour_of_day = -1
        month_of_year = -1
        try:
            dt_obj = datetime.now()
            if isinstance(date_obj, datetime):
                dt_obj = date_obj
            elif isinstance(date_obj, str) and date_obj:
                dt_obj = parse(date_obj)
            else:
                dt_obj = parse(date_obj)
            # Get the day of the week
            day_of_week = dt_obj.isocalendar()[2]
            # Get the hour of the day
            hour_of_day = dt_obj.hour
            # Get the month of the year
            month_of_year = dt_obj.month
        except Exception as err:
            raise ValueError(f"Could not parse date {date_obj}") from err
        return pd.Series([month_of_year, day_of_week, hour_of_day])

    @standard_params("provider_details")
    def _add_provider_details(self, data_df: pd.DataFrame, **kwargs):
        """Checks for the 'dataProvider' and 'dataProviderUrl' fields in the data.

        These are mandatory fields.
        The 'dataProvider' cannot be blank but the 'dataProviderUrl' can be.
        Both fields may be added using the 'provider' dictionary.
        :param data_df: the data to check/add the data provider to
        :param provider: a dictionary containing the 'dataProvider' and 'dataProviderUrl' fields
        :raises ValueError: if the 'dataProvider' field exists but not all rows have valid values
        :raises ValueError: if there is no 'dataProvider' field in the data and provider is None
        :raises ValueError: if there is not 'dataProvider' fields in the data and the provider
        dictionary is supplied with and invalid value for the 'dataProvider' key.
        """
        provider = kwargs.get("provider_details")
        df_cols = data_df.columns
        if "dataProvider" in df_cols:
            empty_dps = data_df[(data_df["dataProvider"].isnull()) | (data_df["dataProvider"] == "")].index
            if not empty_dps.empty:
                empty_dp_recs = data_df.iloc[empty_dps].to_dict("records")
                raise ValueError(
                    'Records in the following rows: %s, contain invalid "dataProvider" values.', list(empty_dp_recs)
                )
        elif "dataProvider" not in df_cols and not provider:
            raise ValueError('Your data or the provider parameter must contain the "dataProvider" field.')
        elif "dataProvider" not in df_cols and provider:
            data_provider = provider.get("dataProvider")
            if not data_provider:
                raise ValueError('The "dataProvider" field must contain a valid string.')
            else:
                data_df["dataProvider"] = data_provider

        if "dataProviderUrl" not in df_cols and not provider:
            data_df["dataProviderUrl"] = ""
        elif "dataProviderUrl" not in df_cols and provider:
            dataProviderUrl = provider.get("dataProviderUrl", "")
            data_df["dataProviderUrl"] = dataProviderUrl

    def _create_unique_id(self, row: dict, unique_fields) -> str:
        """Creates a unique id for each record based on the provided field list.

        If the fields list is empty, it will create the unique id based on all
        values in the record.
        :param record: the record to create the unique id for
        :param unique_fields: the list of field names whose values will be used to create
        the unique id
        :return: the unique sha256 hex representation of the provided string.
        """
        a_str = ""
        values = []
        values = [row.get(key) for key in sorted(unique_fields)] if unique_fields else list(row.values())
        a_str = "".join(str(val) for val in values)
        return hashlib.sha256(a_str.encode("utf-8")).hexdigest().upper()

    @standard_params("unique_fields")
    def _bulkify(self, records: list[dict], index_name: str, **kwargs) -> list:
        """Formats a data frame into the proper structure.

        That structure is generally intended to be ingested using the
        opensearch.helpers bulk api.
        :param records: the list of recordss to bulkify
        :param index_name: the name of the index data should be written to
        :param unique_fields: a list of unique fields used to create a unique id for each record
        :return: the list of dictionaries.
        """
        bulkified_data = []
        unique_fields = kwargs.get("unique_fields")
        for rec in records:
            bulkified_data.append(
                {"_index": index_name, "_id": self._create_unique_id(rec, unique_fields), "_source": rec}
            )
        return bulkified_data

    def _transform_index_suffix(self, index_df: pd.DataFrame, version: int = 1) -> dict:
        """Create OpenSearch index names.

        Index names are created based on the configured ``index_partition_date_format`` and
        map each record to its appropriate index.
        :paramindex_df : dataFrame that contains at least ``self.date_field``.
        :paramv
        :return: dict where each record only contains non-empty key/value pairs.
        """
        data_map = {}

        # No time-based partitioning (none / timeless)
        if self.index_partition_date_format in {"none", "timeless"}:
            index_df["index"] = f"{self.alias}_v{version}_all_time"

        else:
            index_df[self.date_field] = pd.to_datetime(index_df[self.date_field])

            # year (always needed)
            index_df["year"] = index_df[self.date_field].dt.year

            # month with zero-padding (01-12)
            index_df["month"] = index_df[self.date_field].dt.strftime("%m")

            # week of year with zero-padding (01-53)
            index_df["week"] = index_df[self.date_field].dt.isocalendar().week.astype(str).str.zfill(2)

            # day with zero-padding (01-31) - used only for daily partitioning
            index_df["day"] = index_df[self.date_field].dt.strftime("%d")

            # Build the index name according to the selected format
            fmt = self.index_partition_date_format
            if fmt == "yearly":
                index_df["index"] = index_df.apply(lambda r: f"{self.alias}_v{version}_{r['year']}", axis=1)
            elif fmt == "monthly":
                index_df["index"] = index_df.apply(lambda r: f"{self.alias}_v{version}_{r['year']}{r['month']}", axis=1)
            elif fmt == "weekly":
                index_df["index"] = index_df.apply(lambda r: f"{self.alias}_v{version}_{r['year']}{r['week']}", axis=1)
            elif fmt == "daily":  # ← **new branch**
                index_df["index"] = index_df.apply(
                    lambda r: f"{self.alias}_v{version}_{r['year']}{r['month']}{r['day']}",
                    axis=1,
                )
            else:
                raise ValueError(f"Unsupported index_partition_date_format: {self.index_partition_date_format}")

        # Group rows by the generated index and build the final map
        for idx, grp in index_df.groupby("index"):
            # Replace NaNs with empty strings (regex=True works on all dtypes)
            grp = grp.replace(np.nan, "", regex=True)

            # Convert to list of dicts
            records = grp.to_dict("records")

            # Keep only key/value pairs that are not None / empty string
            cleaned = [{k: v for k, v in rec.items() if v is not None and v != ""} for rec in records]

            if idx in data_map:
                data_map[idx].extend(cleaned)
            else:
                data_map[idx] = cleaned

        # Store the list of generated indices on the instance (useful for later steps)
        self.indices = list(data_map.keys())
        return data_map

    def _transform_index_suffix_orig(self, index_df: pd.DataFrame, version: int = 1) -> dict:
        """Called as part of the standardize and bulkify logic.

        It will create indices based on the index partition data format and create a map of index
        names to data. In the case where no partition format is specified or 'all time' in specified,
        a single index will be created and all data will be assocaited with that index. In the case
        where data already exists, the index that gets created in this method should match an existing
        opensearch index.
        :param version: the version may increase if data is reindexed, defaults to 1
        :return: the original data records mapped to indices based on the version and
                 index_partition_date_format.
        """
        data_map = {}  # type: dict
        if self.index_partition_date_format == "none" or self.index_partition_date_format == "timeless":
            index_df["index"] = f"{self.alias}_v{version}_all_time"
        else:
            index_df[self.date_field] = pd.to_datetime(index_df[self.date_field])
            index_df["week"] = index_df[self.date_field].apply(
                lambda dt: dt.weekofyear if dt.weekofyear > 9 else f"0{dt.weekofyear}"
            )
            index_df["month"] = index_df[self.date_field].apply(lambda dt: dt.month if dt.month > 9 else f"0{dt.month}")
            index_df["year"] = index_df[self.date_field].dt.year
            if self.index_partition_date_format == "yearly":
                index_df["index"] = index_df.apply(lambda row: f"{self.alias}_v{version}_{row['year']}", axis=1)
            elif self.index_partition_date_format == "monthly":
                index_df["index"] = index_df.apply(
                    lambda row: (f"{self.alias}_v{version}_{row['year']}{row['month']}"), axis=1
                )
            elif self.index_partition_date_format == "weekly":
                index_df["index"] = index_df.apply(
                    lambda row: (f"{self.alias}_v{version}_{row['year']}{row['week']}"), axis=1
                )
        for index, a_df in index_df.groupby(by="index"):
            # Remove nan's introduced by combining all records into a single data frame
            a_df = a_df.replace(np.nan, "", regex=True)
            recs = a_df.to_dict("records")
            # Only keep key/value pairs in each record that contain a valid value
            recs = [{k: v for k, v in record.items() if v is not None and v != ""} for record in recs]
            if index in data_map:
                data_map[index].extend(recs)
            else:
                data_map[index] = recs
        self.indices = list(data_map.keys())
        return data_map

    @property
    def indices(self) -> list[str]:
        return self._indices

    @indices.setter
    def indices(self, indices: list[str]):
        self._indices = indices
