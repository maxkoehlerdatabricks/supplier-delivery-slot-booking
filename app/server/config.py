import os
import logging
from functools import lru_cache

logger = logging.getLogger(__name__)


class Config:
    """Configuration for Lakebase connection using Databricks SDK.

    Uses the Databricks Python SDK to dynamically resolve the Lakebase
    endpoint host and generate OAuth database credentials. The SDK is
    pre-authenticated in the Databricks Apps environment via the app's
    service principal.

    The Postgres username is the app's service principal client ID,
    available via the DATABRICKS_CLIENT_ID env var (auto-set in Apps).
    """

    def __init__(self):
        self.project = os.getenv("LAKEBASE_PROJECT", "delivery-slot-booking")
        self.db_name = os.getenv("PGDATABASE", os.getenv("LAKEBASE_DB", "delivery_app"))
        self.branch = os.getenv("LAKEBASE_BRANCH", "production")
        self.endpoint = os.getenv("LAKEBASE_ENDPOINT", "primary")

    def get_db_host(self) -> str:
        """Get Lakebase endpoint host via SDK."""
        from databricks.sdk import WorkspaceClient

        w = WorkspaceClient()
        response = w.api_client.do(
            "GET",
            f"/api/2.0/postgres/projects/{self.project}/branches/{self.branch}/endpoints",
        )
        endpoints = response.get("endpoints", [])
        if not endpoints:
            raise RuntimeError(
                f"No endpoints found for branch '{self.branch}' "
                f"in project '{self.project}'"
            )
        host = endpoints[0]["status"]["hosts"]["host"]
        logger.info(f"Resolved Lakebase host: {host}")
        return host

    def get_db_credentials(self) -> tuple[str, str]:
        """Generate OAuth database credential and get username via SDK.

        The username is the service principal's client ID, available as
        DATABRICKS_CLIENT_ID in the Databricks Apps environment.
        """
        from databricks.sdk import WorkspaceClient

        w = WorkspaceClient()

        endpoint_path = (
            f"projects/{self.project}/branches/{self.branch}"
            f"/endpoints/{self.endpoint}"
        )
        response = w.api_client.do(
            "POST",
            "/api/2.0/postgres/credentials",
            body={"endpoint": endpoint_path},
        )
        token = response["token"]

        # Use DATABRICKS_CLIENT_ID (auto-set in Apps for the service principal)
        # as the Postgres username — this matches the Postgres role created for the SP
        username = os.getenv("DATABRICKS_CLIENT_ID")
        if not username:
            # Fallback for local development
            username = w.current_user.me().user_name
            logger.info(f"DATABRICKS_CLIENT_ID not set, falling back to: {username}")
        else:
            logger.info(f"Using service principal client ID as DB user: {username}")

        return token, username


@lru_cache()
def get_config() -> Config:
    return Config()
