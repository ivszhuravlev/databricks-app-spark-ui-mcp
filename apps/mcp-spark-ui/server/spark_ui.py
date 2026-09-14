"""Thin client for Databricks driver-proxy Spark UI REST."""

from __future__ import annotations

import os
import re
from typing import Any
from urllib.parse import urlencode

from databricks.sdk.errors import DatabricksError

from server.utils import get_user_authenticated_workspace_client, get_workspace_client

SPARK_UI_PORT = "40001"
_ADB_ORG = re.compile(r"(?:https?://)?adb-(\d+)\.", re.I)
MAX_STRING = 2000
MAX_LIST = 80

KEEP_METRICS = {
    "duration",
    "scan time",
    "number of output rows",
    "number of files read",
    "number of files written",
    "shuffle bytes written",
    "shuffle records written",
    "shuffle bytes read",
    "shuffle records read",
    "peak memory",
}


def _workspace_host() -> str:
    host = (os.environ.get("DATABRICKS_HOST") or "").strip()
    return host.replace("https://", "").replace("http://", "").rstrip("/")


def _org_id() -> str | None:
    explicit = (os.environ.get("SPARK_UI_ORG_ID") or "").strip()
    if explicit:
        return explicit
    match = _ADB_ORG.match(_workspace_host())
    if match:
        return match.group(1)
    return None


def _proxy_path(cluster_id: str, rest_path: str, query: dict[str, Any] | None = None) -> str:
    rest_path = rest_path.lstrip("/")
    path = (
        f"/driver-proxy-api/o/{_org_id()}/{cluster_id}/"
        f"{SPARK_UI_PORT}/api/v1/{rest_path}"
    )
    if query:
        qs = urlencode({k: str(v) for k, v in query.items() if v is not None})
        if qs:
            path = f"{path}?{qs}"
    return path


def _trim(value: Any, depth: int = 0) -> Any:
    if depth > 8:
        return "<truncated-depth>"
    if isinstance(value, str) and len(value) > MAX_STRING:
        return value[:MAX_STRING] + f"... <truncated {len(value)} chars>"
    if isinstance(value, list):
        trimmed = [_trim(v, depth + 1) for v in value[:MAX_LIST]]
        extra = len(value) - MAX_LIST
        if extra > 0:
            trimmed.append(f"<truncated {extra} more items>")
        return trimmed
    if isinstance(value, dict):
        return {k: _trim(v, depth + 1) for k, v in value.items()}
    return value


def spark_ui_get(cluster_id: str, rest_path: str, query: dict[str, Any] | None = None) -> Any:
    if not cluster_id or not cluster_id.strip():
        return {"error": "cluster_id is required"}
    if not _org_id():
        return {
            "error": "Set SPARK_UI_ORG_ID or DATABRICKS_HOST (Azure adb-<org>....).",
        }
    path = _proxy_path(cluster_id.strip(), rest_path, query)
    clients = (
        get_user_authenticated_workspace_client,
        get_workspace_client,
    )
    last_error = None
    for factory in clients:
        try:
            w = factory()
            # Put GET params on the path. Driver-proxy forwards the raw URI;
            # api_client.do(query=) is easy to drop or flatten on this endpoint.
            data = w.api_client.do("GET", path)
            return _trim(data)
        except DatabricksError as exc:
            last_error = exc
            continue
        except Exception as exc:  # noqa: BLE001 - surface to the MCP client
            last_error = exc
            continue
    return {"error": str(last_error) if last_error else "Spark UI request failed"}


def _metrics_map(metrics: Any) -> dict[str, Any]:
    keep: dict[str, Any] = {}
    if isinstance(metrics, dict):
        for key, value in metrics.items():
            if str(key).strip().lower() in KEEP_METRICS:
                keep[key] = value
        return keep
    if isinstance(metrics, list):
        for item in metrics:
            if not isinstance(item, dict):
                continue
            name = str(item.get("name") or item.get("metric") or "").strip()
            if name.lower() in KEEP_METRICS:
                keep[name] = item.get("value")
    return keep


def summarize_sql(payload: Any) -> Any:
    """Normalize list vs one-execution Spark SQL REST payloads and drop huge plans."""
    if isinstance(payload, list):
        return [summarize_sql(item) for item in payload]
    if not isinstance(payload, dict):
        return payload
    wrapped = payload.get("sql")
    if isinstance(wrapped, list) and "id" not in payload:
        return [summarize_sql(item) for item in wrapped]
    nodes = []
    for node in payload.get("nodes") or []:
        if not isinstance(node, dict):
            continue
        nodes.append(
            {
                "nodeId": node.get("nodeId") or node.get("id"),
                "nodeName": node.get("nodeName") or node.get("name"),
                "metrics": _metrics_map(node.get("metrics")),
            }
        )
    jobs = payload.get("jobs")
    if jobs is None:
        jobs = {
            "running": payload.get("runningJobIds") or [],
            "success": payload.get("successJobIds") or [],
            "failed": payload.get("failedJobIds") or [],
        }
    return {
        "id": payload.get("id"),
        "status": payload.get("status"),
        "success": payload.get("success"),
        "duration": payload.get("duration"),
        "description": payload.get("description"),
        "jobs": jobs,
        "nodes": nodes,
    }


_ENV_KEY_PARTS = (
    "shuffle.partitions",
    "autoBroadcastJoinThreshold",
    "adaptive.",
    "files.maxPartitionBytes",
    "files.openCostInBytes",
    "files.minPartitionNum",
    "broadcastTimeout",
    "serializer",
    "sql.sources",
    "sql.files",
    "aqe",
    "advisoryPartitionSize",
    "coalescePartitions",
    "skewJoin",
    "maxToStringFields",
)


def summarize_environment(payload: Any) -> Any:
    """Keep runtime + the Spark conf that matters for scans, joins, shuffle."""
    if not isinstance(payload, dict):
        return payload
    props = payload.get("sparkProperties") or []
    interesting = []
    for item in props:
        if not isinstance(item, (list, tuple)) or len(item) < 2:
            continue
        key = str(item[0])
        if any(part.lower() in key.lower() for part in _ENV_KEY_PARTS):
            interesting.append({"key": key, "value": item[1]})
    interesting.sort(key=lambda row: row["key"])
    runtime = payload.get("runtime") or {}
    return {
        "runtime": {
            "javaVersion": runtime.get("javaVersion"),
            "scalaVersion": runtime.get("scalaVersion"),
            "sparkVersion": runtime.get("sparkVersion"),
        },
        "sparkProperties": interesting,
        "sparkPropertiesTotal": len(props) if isinstance(props, list) else None,
    }


def summarize_executors(payload: Any) -> Any:
    if not isinstance(payload, list):
        return payload
    rows = []
    total_cores = 0
    active = 0
    for ex in payload:
        if not isinstance(ex, dict):
            continue
        cores = ex.get("totalCores") or 0
        is_active = bool(ex.get("isActive"))
        if is_active:
            active += 1
            total_cores += int(cores)
        rows.append(
            {
                "id": ex.get("id"),
                "isActive": is_active,
                "isDriver": (ex.get("id") == "driver") or bool(ex.get("isDriver")),
                "hostPort": ex.get("hostPort"),
                "totalCores": cores,
                "maxMemory": ex.get("maxMemory"),
                "memoryUsed": ex.get("memoryUsed"),
                "totalDuration": ex.get("totalDuration"),
                "totalGCTime": ex.get("totalGCTime"),
                "totalInputBytes": ex.get("totalInputBytes"),
                "totalShuffleRead": ex.get("totalShuffleRead"),
                "totalShuffleWrite": ex.get("totalShuffleWrite"),
            }
        )
    return {
        "activeExecutors": active,
        "totalCores": total_cores,
        "executors": rows,
    }
