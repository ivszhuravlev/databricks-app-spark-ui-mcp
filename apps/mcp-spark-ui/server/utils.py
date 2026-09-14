import contextvars
import os

from databricks.sdk import WorkspaceClient

header_store = contextvars.ContextVar("header_store", default={})


def get_workspace_client() -> WorkspaceClient:
    """App service principal (Databricks Apps injects OAuth client env)."""
    return WorkspaceClient()


def get_user_authenticated_workspace_client() -> WorkspaceClient:
    """Prefer the caller's forwarded token; fall back to the app SP.

    Apps user_api_scopes typically omit clusters, so OBO driver-proxy often
    fails and spark_ui_get then retries as the app service principal.
    """
    if "DATABRICKS_APP_NAME" not in os.environ:
        return WorkspaceClient()
    headers = header_store.get({}) or {}
    token = headers.get("x-forwarded-access-token")
    if not token:
        return WorkspaceClient()
    return WorkspaceClient(token=token, auth_type="pat")
