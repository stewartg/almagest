- [Development](#development)
- [OpenSearch Client Helper](#opensearch-client-helper)
  - [🔒 Singleton Pattern Implementation](#-singleton-pattern-implementation)
  - [🛠 `ClientHelper` Class](#-clienthelper-class)
    - [1. Environment Variable Configuration](#1-environment-variable-configuration)
    - [2. Client Initialization `get_client`](#2-client-initialization-get_client)
    - [3. Property Access](#3-property-access)
  - [Usage Example](#usage-example)
- [Data Normalizer](#data-normalizer)
  - [🏗 Core Architecture](#-core-architecture)
  - [🔑 Key Features](#-key-features)
    - [1. Strict Parameter Validation](#1-strict-parameter-validation)
    - [2. Data Cleaning & Standardization](#2-data-cleaning--standardization)
    - [3. Dynamic Index Partitioning](#3-dynamic-index-partitioning)
    - [4. Deterministic Unique IDs](#4-deterministic-unique-ids)
    - [5. Bulk API Formatting](#5-bulk-api-formatting)
  - [🚀 Usage Workflow](#-usage-workflow)
  - [⚙️ Configuration Options](#️-configuration-options)
- [Template Generator](#template-generator)
  - [🏗 Core Workflow](#-core-workflow)
  - [🔑 Key Features](#-key-features-1)
    - [1. Dynamic Mapping Inference](#1-dynamic-mapping-inference)
    - [2. Automatic Cleanup](#2-automatic-cleanup)
    - [3. Standardized Settings](#3-standardized-settings)
    - [4. Safety Checks](#4-safety-checks)
  - [🚀 Usage Example](#-usage-example)
  - [⚙️ Method Overview](#️-method-overview)
- [Fluent DSL Client](#fluent-dsl-client)
  - [🏗 Core Architecture](#-core-architecture-1)
  - [🔑 Composed Mixins](#-composed-mixins)
  - [🚀 Usage Example](#-usage-example-1)
  - [🧪 Testing the Fluent Chain & Preferred Usage](#-testing-the-fluent-chain--preferred-usage)


## Development

To be developed/deployed against the latest stable release of Python 3.9 or later


1. Clone this repo, you should be in the /your/cloned/directory/almagest directory. If not, cd to that directory.
2. Source the dev setup script to setup your environment:
1. This script will create a virtual environment, install the required dependencies, activate the new environment and setup pre-commit hooks. If you are using VSCODE, it should recognize the virtual environment install and prompt to configure your python interpreter to use the newly created virtual environment.
1. There are 4 optional arguments:
    * `-v` flag specifies which version of python to use (example: `-v 3.11`)
    * `-d` installs build, dev, and test dependencies in editable mode
    * `-b` installs build dependencies only
    * `-a` installs all dependencies in editable mode/
1. To use the default python version and setup an editable dev environment with all dependencies installed run:

```bash
source scripts/setup_python_environment.sh -a
``` 

## OpenSearch Client Helper

This module provides a robust, singleton-based client manager for connecting to an OpenSearch cluster. It ensures that only one instance of the client configuration exists throughout the application lifecycle, preventing redundant connections and centralizing environment variable management.

### 🔒 Singleton Pattern Implementation

The code utilizes a custom `Singleton` metaclass to enforce the **Singleton Design Pattern**:

*   **`Singleton` Metaclass**: Overrides `__init__` and `__call__` to ensure that `ClientHelper` can only be instantiated once. Subsequent calls to `ClientHelper()` return the original instance.
*   **Benefit**: This guarantees that environment variables are read only once and that the application maintains a single source of truth for connection credentials and host configuration.

### 🛠 `ClientHelper` Class

The `ClientHelper` class encapsulates the logic for initializing and retrieving the OpenSearch client.

#### 1. Environment Variable Configuration
Upon initialization, the class strictly requires the following environment variables to be set:
*   `OPENSEARCH_HOST`
*   `OPENSEARCH_USER`
*   `OPENSEARCH_PW`

If any of these are missing, a `ValueError` is raised immediately, failing fast to prevent runtime connection errors later in the execution flow.

#### 2. Client Initialization (`get_client`)
The `get_client` class method is the primary entry point for obtaining a connected client:
*   **Authentication**: Retrieves stored credentials and constructs an HTTP auth tuple.
*   **Connection Settings**: Configures the `OpenSearch` client with:
    *   **SSL/TLS**: Enabled (`use_ssl=True`, `scheme="https"`) on port `443`.
    *   **Certificate Verification**: Disabled (`verify_certs=False`) with warnings enabled (`ssl_show_warn=True`). *Note: Disabling verification is common in development but should be reviewed for production environments.*
*   **Health Check**: Executes a `client.ping()` to verify connectivity. If the ping fails, a `ValueError` is raised.

#### 3. Property Access
*   **`host`**: A read-only property that exposes the configured OpenSearch host string, allowing other parts of the application to inspect the target endpoint without exposing internal state directly.

### Usage Example

```python
# The first call initializes the singleton and reads env vars
client = ClientHelper.get_client()

# Subsequent calls return the same configured instance
same_client = ClientHelper.get_client()

assert client is same_client  # True
```

## Data Normalizer

This module provides the `DataNormalizer` class, a robust utility designed to standardize, validate, and prepare raw data dictionaries for ingestion into an OpenSearch cluster. It leverages `pandas` for efficient data manipulation and ensures strict schema compliance before bulk indexing.

### 🏗 Core Architecture

The class operates on a list of dictionaries (records) and performs the following high-level tasks:
1.  **Validation**: Ensures mandatory fields (classification, dates, provider info) exist and are valid.
2.  **Normalization**: Cleans data types, handles `NaN`/`None` values, and standardizes date formats.
3.  **Partitioning**: Dynamically generates OpenSearch index names based on time-based strategies (yearly, monthly, weekly, daily).
4.  **Bulkification**: Formats data into the specific JSON structure required by the OpenSearch Bulk API, including generating deterministic unique IDs.

### 🔑 Key Features

#### 1. Strict Parameter Validation
The class uses a custom decorator `@standard_params` to enforce the presence of critical configuration arguments before executing methods like `standardize` or `bulkify`. Missing parameters trigger immediate `ValueError` exceptions.
*   **Required Params**: `classification_field`, `start_date_field`, `provider_details`, `unique_fields`.

#### 2. Data Cleaning & Standardization
The `standardize` method performs several cleanup operations:
*   **Null Handling**: Replaces various `NaN` representations (`np.nan`, `None`, `"nan"`, `"NAN"`) and empty-like strings (only spaces, dashes, or plus signs) with empty strings `""`.
*   **Classification Mapping**: Renames a user-specified classification column to the standard `classification` field.
*   **Date Parsing**: Converts a specified date field into `startTimestamp` and enriches the dataset with derived columns for visualization:
    *   `monthOfYear_zulu`
    *   `dayOfWeek_zulu`
    *   `hourOfDay_zulu`
*   **Provider Injection**: Validates or injects `dataProvider` and `dataProviderUrl` fields, ensuring no records have missing provider information.
*   **Ingest Timestamp**: Automatically adds an `ingestTimestamp` column with the current execution time.

#### 3. Dynamic Index Partitioning
The `_transform_index_suffix` method intelligently routes records to specific indices based on the `index_partition_date_format` configuration:
*   **Supported Formats**:
    *   `none` / `timeless`: All data goes to a single `_all_time` index.
    *   `yearly`: `{alias}_v{version}_{YYYY}`
    *   `monthly`: `{alias}_v{version}_{YYYYMM}`
    *   `weekly`: `{alias}_v{version}_{YYYYww}`
    *   `daily`: `{alias}_v{version}_{YYYYMMDD}`
*   **Logic**: It parses the date field, extracts relevant time components, constructs the index name per record, and groups the data into a dictionary where keys are index names and values are lists of records.

#### 4. Deterministic Unique IDs
To prevent duplicate documents in OpenSearch, the `_create_unique_id` method generates a SHA-256 hash:
*   It sorts the values of specified `unique_fields` (or all fields if none are specified).
*   It concatenates these values into a string.
*   It returns the uppercase hexadecimal hash, which serves as the `_id` in the bulk request.

#### 5. Bulk API Formatting
The `_bulkify` method transforms the cleaned DataFrame rows into the specific action/metadata format required by `opensearchpy.helpers.bulk`:
```python
{
    "_index": "my-index-v1-202310",
    "_id": "A1B2C3D4...", # SHA-256 Hash
    "_source": { ... } # The actual record data
}
```

### 🚀 Usage Workflow

The primary entry point is the `standardize_and_bulkify` method, which chains the entire process:

```python
from data_normalizer import DataNormalizer

raw_data = [
    {"id": 1, "date": "2023-10-27", "type": "A", "provider": "SourceX"},
    {"id": 2, "date": "2023-10-28", "type": "B", "provider": "SourceX"}
]

normalizer = DataNormalizer(
    data=raw_data,
    alias="logs",
    index_partition_date_format="monthly"
)

# Execute the full pipeline
bulk_records = normalizer.standardize_and_bulkify(
    classification_field="type",
    start_date_field="date",
    provider_details={"dataProvider": "SourceX", "dataProviderUrl": "https://source.x"},
    unique_fields=["id"]
)

# bulk_records is now ready for client.bulk()
```

### ⚙️ Configuration Options

| Parameter | Description |
| :--- | :--- |
| `data` | List of input dictionaries. |
| `alias` | Base name for the OpenSearch index alias. |
| `index_partition_date_format` | Strategy for splitting indices: `none`, `timeless`, `yearly`, `monthly`, `weekly`, `daily`. |
| `index_version` | Integer version number appended to index names (useful for reindexing). |


## Template Generator

This module provides the `TemplateGenerator` class, a utility designed to automate the creation of **OpenSearch Index Templates** based on sample data. By analyzing a representative dataset, it leverages OpenSearch's dynamic mapping inference to generate robust, reusable templates that ensure schema consistency for future data ingestion.

### 🏗 Core Workflow

The class orchestrates the following lifecycle:
1.  **Validation**: Verifies input arguments (`alias`, date format, standard parameters).
2.  **Normalization**: Processes sample data using `DataNormalizer` to ensure schema compliance.
3.  **Mapping Inference**: Bulk indexes the normalized data into a **temporary index**, allowing OpenSearch to automatically infer field types and mappings.
4.  **Template Construction**: Extracts the generated mappings and wraps them in a formal template body with standardized settings (shards, replicas, aliases).
5.  **Cleanup & Registration**: Deletes the temporary index and registers the new template under the specified alias.

### 🔑 Key Features

#### 1. Dynamic Mapping Inference
Instead of manually defining complex mapping JSON, this class uses real data to let OpenSearch determine the optimal field types. This reduces human error and adapts to evolving data structures.

#### 2. Automatic Cleanup
The process is non-destructive to the cluster's storage. A temporary index is created solely for the analysis phase and is **automatically deleted** once the mappings are extracted and the template is saved.

#### 3. Standardized Settings
The generated template enforces consistent cluster configurations:
*   **Shards**: Fixed to `1`.
*   **Replicas**: Fixed to `2`.
*   **Aliases**: Automatically associates the template with the provided alias name.
*   **Patterns**: Applies to all indices matching `{alias}_*`.

#### 4. Safety Checks
*   **Existence Check**: Before generating, it checks if a template with the same name already exists to prevent accidental overwrites.
*   **Argument Validation**: Strictly validates `index_details` and `standard_params` before execution to fail fast on configuration errors.
*   **Index Verification**: Ensures all target indices derived from the normalizer exist (creating them if necessary) before attempting bulk operations.

### 🚀 Usage Example

```python
from almagest.template_generator import TemplateGenerator

# Sample data representing the final schema
sample_data = [
    {"id": 1, "timestamp": "2023-10-27T10:00:00Z", "status": "active", "count": 42},
    {"id": 2, "timestamp": "2023-10-28T11:30:00Z", "status": "inactive", "count": 15}
]

generator = TemplateGenerator()

try:
    generator.generate_template(
        data=sample_data,
        index_details={
            "alias": "app-logs",
            "index_partition_date_format": "monthly",
            "index_version": 1
        },
        standard_params={
            "classification_field": "status",
            "start_date_field": "timestamp",
            "provider_details": {"dataProvider": "MyApp", "dataProviderUrl": "https://myapp.com"},
            "unique_fields": ["id"]
        }
    )
    # Output: Template 'app-logs' successfully created.
except ValueError as e:
    print(f"Generation failed: {e}")
```

## Fluent DSL Client

The `FluentDslClient` is the primary interface for constructing and executing complex OpenSearch queries. It composes multiple functional mixins to provide a unified, chainable API that handles matching, aggregation, date filtering, and pagination seamlessly.

### 🏗 Core Architecture

Instead of creating monolithic wrapper classes, the client inherits from a hierarchy of specialized mixins. This allows for modular functionality while maintaining a single, coherent object state. The class combines `MatchMixin`, `AggMixin`, `DateMixin`, `PagerMixin`, and `BaseMixin` to offer a comprehensive toolkit for query building.

### 🔑 Composed Mixins

- **`MatchMixin`**: Handles boolean logic (`must`, `filter`, `must_not`), term matches, and existence checks.
- **`DateMixin`**: Provides helpers for range queries (`between`, `after`, `before`) with automatic ISO formatting.
- **`AggMixin`**: Manages complex aggregations, specifically Point-in-Time (PIT) based composite aggregations for deep pagination and "latest/earliest" document retrieval.
- **`PagerMixin`**: Orchestrates the `search_after` loop to fetch large datasets efficiently without deep pagination penalties.
- **`BaseMixin`**: Initializes the underlying `opensearchpy.Search` object, manages the client connection, and handles shared state.

### 🚀 Usage Example

```python
from almagest.client import FluentDslClient
import datetime as dt

# Initialize the client targeting a specific index
client = FluentDslClient(index="app-logs-v1")

start = dt.datetime(2026, 3, 1)
end = dt.datetime(2026, 3, 6)

# Chain methods to build a complex query
results = (
    client
    .between("timestamp", start, end)          # DateMixin: Range filter
    .exactly("status", "error")                # MatchMixin: Term filter
    .search_after(timeout=30)                  # PagerMixin: Execute with pagination
)

# 'results' contains the flattened list of hits from all pages
for hit in results:
    print(f"Error at {hit['timestamp']}: {hit['msg']}")
```

### 🧪 Testing the Fluent Chain & Preferred Usage

The `FluentDslClient` is rigorously tested to ensure state is correctly passed between mixins and that the final DSL generation matches expectations. 

**Preferred Usage Pattern:**
The standard workflow is to chain all configuration methods (filtering, dating, aggregating) and terminate the chain with the **`search_after()`** method. This method acts as the execution trigger, handling the internal pagination loop automatically.

*   **Why `search_after()`?** Unlike standard `.execute()` calls, `search_after()` manages the cursor-based pagination loop internally. It fetches the first page, extracts the sort cursor, and continues fetching subsequent pages until the dataset is exhausted (or a limit is reached), returning a flattened list of results.
*   **Reference:** See `test_fluent_chain_with_pagination_loop` for the canonical implementation pattern.

**Key Test Scenarios:**
1.  **Date & Match Integration**: Verifies that `between()` correctly formats ISO dates and combines them with `exactly()` term queries before execution.
2.  **Pagination Loop**: Ensures `search_after()` correctly extracts cursors from responses and iterates until no more results are found.
3.  **Aggregation Setup**: Confirms that `latest()` configures the composite aggregation sources with correct keyword suffixes (e.g., `user_id.keyword`) prior to the `search_after()` call.
4.  **Complex Chains**: Validates that combining Date, Match, and Agg mixins results in a valid DSL body containing `query`, `aggs`, and `pit` (Point-in-Time) blocks when executed.

*Example Test Assertion Logic:*
```python
# 1. Chain configuration methods
# (DateMixin)
assert len(dsl_client._range_calls) == 2
fields = [call[0] for call in dsl_client._range_calls]
assert fields == ["timestamp", "timestamp"]

# (MatchMixin)
assert len(dsl_client._must) == 1
term_query = dsl_client._must[0]
expected = {"term": {"status": "error"}}

# 2. Execute via search_after() (The Preferred Trigger)
# This triggers the internal loop defined in PagerMixin
results = dsl_client.search_after(timeout=30)

# 3. Verify Execution State
# The PagerMixin should have updated internal args with the cursor from the last hit
assert dsl_client._search._extra_args.get("search_after") == ["cursor_abc"]

# 4. Verify Final DSL Structure (if inspecting raw DSL)
dsl_client.pit_id = "pit-xyz"
final_body = dsl_client.to_dict()
assert "aggs" in final_body
assert final_body.get("size") == 0 # Size 0 is typical for pure aggregation queries

# Test Release
