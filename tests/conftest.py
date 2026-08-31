"""Session-scoped Spark fixture for all tests.

Uses databricks-connect's DatabricksSession (remote), not a local pyspark session -
databricks-connect and plain pyspark conflict when both installed in the same environment
(confirmed: installing databricks-connect breaks pyspark's local-master mode), and
databricks-connect is already this project's dev dependency for local development (see
README.md's "Local Python environment" section). So these tests need a working CLI
profile/reachable compute to run, same as `databricks-connect test` already requires - not a
new constraint.

Picks up whatever profile/auth is active in the environment (DATABRICKS_CONFIG_PROFILE, or
the CLI's configured default) - no separate profile-selection logic here.
"""

import pytest
from databricks.connect import DatabricksSession


@pytest.fixture(scope="session")
def spark():
    return DatabricksSession.builder.getOrCreate()
