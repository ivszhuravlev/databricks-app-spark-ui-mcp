"""MCP tools that read classic Spark UI through Databricks driver-proxy."""

from server.spark_ui import (
    spark_ui_get,
    summarize_environment,
    summarize_executors,
    summarize_sql,
)


def _is_error(data) -> bool:
    return isinstance(data, dict) and "error" in data


def _task_sort_by(sort_by: str) -> str:
    """Spark REST sortBy is runtime|-runtime, not the UI column name Duration."""
    raw = (sort_by or "Duration").strip()
    key = raw.lower()
    if key in {"duration", "-runtime", "decreasing_runtime"}:
        return "-runtime"
    if key in {"runtime", "increasing_runtime"}:
        return "runtime"
    if key in {"id"}:
        return "ID"
    if raw in {"DECREASING_RUNTIME", "INCREASING_RUNTIME", "ID"}:
        return "-runtime" if raw == "DECREASING_RUNTIME" else (
            "runtime" if raw == "INCREASING_RUNTIME" else "ID"
        )
    return "-runtime"


def load_tools(mcp_server) -> None:
    @mcp_server.tool
    def health() -> dict:
        """Confirm the Spark UI MCP server is running."""
        return {
            "status": "healthy",
            "message": "mcp-spark-ui is up. Use spark_ui_* tools to read a classic cluster Spark UI.",
        }

    @mcp_server.tool
    def spark_ui_applications(cluster_id: str) -> dict:
        """List Spark applications on a classic Databricks cluster Spark UI.

        cluster_id is the compute id from the workspace Compute page, not the
        browser spark_context_id.
        """
        data = spark_ui_get(cluster_id, "applications")
        if _is_error(data):
            return data
        return {"cluster_id": cluster_id, "applications": data}

    @mcp_server.tool
    def spark_ui_sql(
        cluster_id: str,
        application_id: str,
        execution_id: int | None = None,
    ) -> dict:
        """Read Spark SQL executions from Spark UI.

        Omit execution_id to list recent SQL executions. Pass execution_id to
        get one execution: duration, jobs, and node metrics (files, rows, join
        operator names).
        """
        if not application_id:
            return {"error": "application_id is required"}
        if execution_id is None:
            data = spark_ui_get(
                cluster_id,
                f"applications/{application_id}/sql",
                query={
                    "offset": 0,
                    "length": 50,
                    "details": "false",
                    "planDescription": "false",
                },
            )
            if _is_error(data):
                return data
            return {
                "cluster_id": cluster_id,
                "application_id": application_id,
                "sql": summarize_sql(data),
            }
        data = spark_ui_get(
            cluster_id,
            f"applications/{application_id}/sql/{execution_id}",
            query={"details": "true", "planDescription": "false"},
        )
        if _is_error(data):
            return data
        return {
            "cluster_id": cluster_id,
            "application_id": application_id,
            "sql": summarize_sql(data),
        }

    @mcp_server.tool
    def spark_ui_environment(cluster_id: str, application_id: str) -> dict:
        """Read Spark UI Environment: effective sparkProperties (shuffle, files, broadcast, AQE).

        Use this before blaming notebook code. Cluster defaults apply even when
        the notebook sets nothing.
        """
        if not application_id:
            return {"error": "application_id is required"}
        data = spark_ui_get(cluster_id, f"applications/{application_id}/environment")
        if _is_error(data):
            return data
        return {
            "cluster_id": cluster_id,
            "application_id": application_id,
            "environment": summarize_environment(data),
        }

    @mcp_server.tool
    def spark_ui_executors(cluster_id: str, application_id: str) -> dict:
        """Read Spark UI executors: active count, total cores, memory, GC, shuffle."""
        if not application_id:
            return {"error": "application_id is required"}
        data = spark_ui_get(cluster_id, f"applications/{application_id}/allexecutors")
        if _is_error(data):
            return data
        return {
            "cluster_id": cluster_id,
            "application_id": application_id,
            "executors": summarize_executors(data),
        }

    @mcp_server.tool
    def spark_ui_job(
        cluster_id: str,
        application_id: str,
        job_id: int,
    ) -> dict:
        """Read one Spark job from Spark UI (status, stageIds, task counts)."""
        data = spark_ui_get(
            cluster_id,
            f"applications/{application_id}/jobs/{job_id}",
        )
        if _is_error(data):
            return data
        return {
            "cluster_id": cluster_id,
            "application_id": application_id,
            "job": data,
        }

    @mcp_server.tool
    def spark_ui_stage(
        cluster_id: str,
        application_id: str,
        stage_id: int,
        attempt_id: int = 0,
    ) -> dict:
        """Read one Spark stage from Spark UI (task counts, shuffle, status)."""
        data = spark_ui_get(
            cluster_id,
            f"applications/{application_id}/stages/{stage_id}/{attempt_id}",
            query={"details": "false"},
        )
        if _is_error(data):
            return data
        return {
            "cluster_id": cluster_id,
            "application_id": application_id,
            "stage": data,
        }

    @mcp_server.tool
    def spark_ui_task_list(
        cluster_id: str,
        application_id: str,
        stage_id: int,
        attempt_id: int = 0,
        offset: int = 0,
        length: int = 50,
        sort_by: str = "Duration",
    ) -> dict:
        """Read Spark stage tasks. sort_by=Duration puts the slowest tasks first.

        Use this to see duration skew: compare the first task duration to later
        tasks, and records read/written per task.
        """
        length = max(1, min(int(length), 200))
        data = spark_ui_get(
            cluster_id,
            f"applications/{application_id}/stages/{stage_id}/{attempt_id}/taskList",
            query={
                "offset": offset,
                "length": length,
                "sortBy": _task_sort_by(sort_by),
            },
        )
        if _is_error(data):
            return data
        summary = None
        if isinstance(data, list) and data:
            durations = [t.get("duration") for t in data if isinstance(t, dict) and t.get("duration") is not None]
            records = []
            for task in data:
                if not isinstance(task, dict):
                    continue
                metrics = task.get("taskMetrics") or {}
                records.append(metrics.get("inputMetrics", {}).get("recordsRead") or 0)
            summary = {
                "returned_tasks": len(data),
                "max_duration_ms": max(durations) if durations else None,
                "min_duration_ms": min(durations) if durations else None,
                "max_records_read": max(records) if records else None,
            }
        return {
            "cluster_id": cluster_id,
            "application_id": application_id,
            "stage_id": stage_id,
            "summary": summary,
            "tasks": data,
        }
