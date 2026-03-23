import os
import json
import subprocess
from functools import lru_cache


class Config:
    """Configuration for Lakebase connection with dual-mode auth."""

    def __init__(self):
        self.project = os.getenv("LAKEBASE_PROJECT", "delivery-slot-booking")
        self.db_name = os.getenv("LAKEBASE_DB", "delivery_app")
        self.branch = os.getenv("LAKEBASE_BRANCH", "production")
        self.endpoint = os.getenv("LAKEBASE_ENDPOINT", "primary")
        self.profile = os.getenv(
            "DATABRICKS_PROFILE", "fe-vm-fevm-serverless-stable-nyu9oz"
        )
        self._is_app_mode = os.getenv("DATABRICKS_APP_NAME") is not None

    @property
    def is_app_mode(self) -> bool:
        return self._is_app_mode

    def get_db_host(self) -> str:
        """Get Lakebase endpoint host."""
        result = subprocess.run(
            [
                "databricks",
                "postgres",
                "list-endpoints",
                f"projects/{self.project}/branches/{self.branch}",
                "-p",
                self.profile,
                "-o",
                "json",
            ],
            capture_output=True,
            text=True,
        )
        endpoints = json.loads(result.stdout)
        return endpoints[0]["status"]["hosts"]["host"]

    def get_db_credentials(self) -> tuple[str, str]:
        """Get OAuth token and user email."""
        endpoint_path = f"projects/{self.project}/branches/{self.branch}/endpoints/{self.endpoint}"

        # Token
        result = subprocess.run(
            [
                "databricks",
                "postgres",
                "generate-database-credential",
                endpoint_path,
                "-p",
                self.profile,
                "-o",
                "json",
            ],
            capture_output=True,
            text=True,
        )
        token = json.loads(result.stdout)["token"]

        # Email
        result = subprocess.run(
            [
                "databricks",
                "current-user",
                "me",
                "-p",
                self.profile,
                "-o",
                "json",
            ],
            capture_output=True,
            text=True,
        )
        email = json.loads(result.stdout)["userName"]

        return token, email

    def get_app_credentials(self) -> tuple[str, str]:
        """Get credentials in Databricks App mode using the SDK."""
        from databricks.sdk import WorkspaceClient

        w = WorkspaceClient()

        # Get host from endpoint
        host = self.get_db_host()

        # In app mode, use the app's service principal token
        token = w.config.authenticate()
        email = w.current_user.me().user_name

        return token, email


@lru_cache()
def get_config() -> Config:
    return Config()
