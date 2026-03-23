import os
import json
import subprocess
import logging
from functools import lru_cache

logger = logging.getLogger(__name__)


class Config:
    """Configuration for Lakebase connection with dual-mode auth.

    - **App mode** (DATABRICKS_APP_NAME set): uses the Databricks Python SDK
      which auto-authenticates via the service principal injected by the runtime.
    - **Local mode**: shells out to `databricks` CLI with the configured profile.
    """

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
        if self._is_app_mode:
            return self._get_db_host_sdk()
        return self._get_db_host_cli()

    def get_db_credentials(self) -> tuple[str, str]:
        """Get OAuth token and user email for Lakebase connection."""
        if self._is_app_mode:
            return self._get_credentials_sdk()
        return self._get_credentials_cli()

    # ── SDK mode (Databricks App) ────────────────────────────────

    def _get_db_host_sdk(self) -> str:
        from databricks.sdk import WorkspaceClient

        w = WorkspaceClient()
        branch_path = f"projects/{self.project}/branches/{self.branch}"
        resp = w.api_client.do("GET", f"/api/2.0/databases/postgres/{branch_path}/endpoints")
        endpoints = resp.get("endpoints", []) if isinstance(resp, dict) else []
        if not endpoints:
            raise RuntimeError("No Lakebase endpoints found via SDK")
        return endpoints[0]["status"]["hosts"]["host"]

    def _get_credentials_sdk(self) -> tuple[str, str]:
        from databricks.sdk import WorkspaceClient

        w = WorkspaceClient()
        endpoint_path = (
            f"projects/{self.project}/branches/{self.branch}"
            f"/endpoints/{self.endpoint}"
        )
        resp = w.api_client.do(
            "POST",
            f"/api/2.0/databases/postgres/{endpoint_path}:generateCredential",
        )
        token = resp["token"]
        email = w.current_user.me().user_name
        return token, email

    # ── CLI mode (local development) ─────────────────────────────

    def _get_db_host_cli(self) -> str:
        result = subprocess.run(
            [
                "databricks", "postgres", "list-endpoints",
                f"projects/{self.project}/branches/{self.branch}",
                "-p", self.profile, "-o", "json",
            ],
            capture_output=True, text=True,
        )
        if result.returncode != 0:
            raise RuntimeError(f"Failed to list endpoints: {result.stderr}")
        endpoints = json.loads(result.stdout)
        return endpoints[0]["status"]["hosts"]["host"]

    def _get_credentials_cli(self) -> tuple[str, str]:
        endpoint_path = (
            f"projects/{self.project}/branches/{self.branch}"
            f"/endpoints/{self.endpoint}"
        )
        result = subprocess.run(
            [
                "databricks", "postgres", "generate-database-credential",
                endpoint_path, "-p", self.profile, "-o", "json",
            ],
            capture_output=True, text=True,
        )
        if result.returncode != 0:
            raise RuntimeError(f"Failed to generate credential: {result.stderr}")
        token = json.loads(result.stdout)["token"]

        result = subprocess.run(
            [
                "databricks", "current-user", "me",
                "-p", self.profile, "-o", "json",
            ],
            capture_output=True, text=True,
        )
        if result.returncode != 0:
            raise RuntimeError(f"Failed to get current user: {result.stderr}")
        email = json.loads(result.stdout)["userName"]

        return token, email


@lru_cache()
def get_config() -> Config:
    return Config()
