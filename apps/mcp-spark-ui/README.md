# mcp-spark-ui

Minimal Databricks App MCP that reads classic cluster Spark UI via driver-proxy.

MCP endpoint: `https://<app-url>/mcp`

Org id and CORS come from `DATABRICKS_HOST` (Apps injects it) or optional `SPARK_UI_ORG_ID` / `SPARK_UI_CORS_ORIGINS`. Callers pass `cluster_id`.

Tools: `health`, `spark_ui_applications`, `spark_ui_sql`, `spark_ui_environment`, `spark_ui_executors`, `spark_ui_job`, `spark_ui_stage`, `spark_ui_task_list`.
