import os
import json
import subprocess
import logging
from functools import lru_cache

logger = logging.getLogger(__name__)


class Config:
    """Configuration for Lakebase connection with dual-mode auth.

    - **App mode** (PGHOST set): uses PG* env vars with native Postgres
      password auth via a Databricks secret.
    - **Local mode**: shells out to `databricks` CLI for OAuth tokens.
    """

    def __init__(self):
        self.project = os.getenv("LAKEBASE_PROJECT", "delivery-slot-booking")
        self.db_name = os.getenv("PGDATABASE", os.getenv("LAKEBASE_DB", "delivery_app"))
        self.branch = os.getenv("LAKEBASE_BRANCH", "production")
        self.endpoint = os.getenv("LAKEBASE_ENDPOINT", "primary")
        self.profile = os.getenv(
            "DATABRICKS_PROFILE", "fe-vm-fevm-serverless-stable-nyu9oz"
        )
        self._pg_host = os.getenv("PGHOST")
        self._pg_user = os.getenv("PGUSER")
        self._pg_password = os.getenv("PGPASSWORD")
        self._is_app_mode = self._pg_host is not None

    @property
    def is_app_mode(self) -> bool:
        return self._is_app_mode

    def get_db_host(self) -> str:
        """Get Lakebase endpoint host."""
        if self._is_app_mode:
            logger.info(f"App mode: using PGHOST={self._pg_host}")
            return self._pg_host
        return self._get_db_host_cli()

    def get_db_credentials(self) -> tuple[str, str]:
        """Get password/token and username for Lakebase connection."""
        if self._is_app_mode:
            logger.info(f"App mode: using native password auth as {self._pg_user}")
            return self._pg_password, self._pg_user
        return self._get_credentials_cli()

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
