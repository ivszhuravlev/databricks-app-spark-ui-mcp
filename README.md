# Databricks App: Spark UI MCP

Read-only MCP server for **classic compute Spark UI**. Deploy it as a Databricks App, then attach it in Genie Code.

## The problem

Genie Code can edit notebooks and reason about source. It cannot natively open classic cluster Spark UI (Jobs, Stages, SQL, Environment, task lists). The [Genie Code MCP catalog](https://docs.databricks.com/aws/en/genie-code/mcp) has no Spark UI server.

I needed that for a slow Spark job and did not want to click through the UI by hand. Portable “read the Spark UI” skills alone do nothing: the agent still has no tool.

## What this is

A small **Databricks App** (`apps/mcp-spark-ui`) that serves MCP at `/mcp` with `stateless_http=True` ([custom MCP](https://docs.databricks.com/aws/en/agents/mcp-tools/custom-mcp)).

It calls Spark UI REST on the cluster **driver** through Databricks **driver-proxy** (`/driver-proxy-api/.../40001/api/v1/...`). You pass `cluster_id` from the Compute page, not the browser `spark_context_id`.

Tools: `health`, `spark_ui_applications`, `spark_ui_sql`, `spark_ui_environment`, `spark_ui_executors`, `spark_ui_job`, `spark_ui_stage`, `spark_ui_task_list`.

On a spoiled eval job in this repo, Genie Code + the skills below + this App got the write from **~312s to ~92s**, notebook-only. That is why I am sharing the App. You do not need the eval to use it on your own jobs.

Limits: live classic driver only. Not serverless, not SQL warehouse query profile, not Spark History Server.

## Deploy the App on your workspace

1. Copy `apps/mcp-spark-ui` into your workspace (Repos / `databricks bundle` / import — whatever you already use).
2. Create a **Databricks App** from that folder. The process command is in `app.yaml`: `uv run custom-mcp-server`.
3. Apps inject `DATABRICKS_HOST`. Org id for driver-proxy is taken from that host (`adb-<org>.…`). Override with `SPARK_UI_ORG_ID` only if you must. CORS defaults to `DATABRICKS_HOST`; override with `SPARK_UI_CORS_ORIGINS` (comma-separated) if the workspace URL and App origin differ.
4. Give the **app service principal** permission on the classic cluster you will debug (`CAN_ATTACH_TO` is the usual minimum so driver-proxy works). User OBO often cannot call clusters; the App falls back to this SP.
5. App URL is `https://<app-name>-<org>.<cloud>.databricksapps.com`. MCP endpoint: `https://<app-url>/mcp`.
6. In **Genie Code → Settings → MCP servers**, add this App as a custom MCP server. Enable the `spark_ui_*` tools (not only `health`).
7. Optional: attach `skills/spark-performance-tuning` and `skills/spark-debugging` as Genie Code skills. They stay generic; they do not describe your job.

## Use it on your job

Classic cluster must be **Running** (driver up). In Genie Code (Agent mode), point at that cluster id and the job/run you care about. Typical order:

1. `spark_ui_applications` — live `application_id`
2. `spark_ui_sql` — pick the SQL that owns wall time
3. `spark_ui_environment` — effective conf (`maxPartitionBytes`, broadcast threshold, AQE, `shuffle.partitions`)
4. `spark_ui_job` / `spark_ui_stage` / `spark_ui_task_list` — files, rows, join type, skew

Do not expect History Server. After a cluster restart the old Spark app is gone.

## Check that the App works

- Browser: `GET https://<app-url>/health` → `{"status":"healthy"}`
- MCP: `tools/list` on `/mcp` should return the `spark_ui_*` tools
- `spark_ui_applications` with your `cluster_id` should return the current Spark app while the cluster is running

If tools/list works but applications/SQL return 403, the app SP cannot reach driver-proxy on that cluster.

---

## Optional: eval in this repo

Only if you want to reproduce the number above. Skip this on a real project.

1. `notebooks/prepare_data.py` — clean work tables.
2. `notebooks/spoil_data.py` — tiny files, store-key skew, 6-vendor FX.
3. Set this **on the classic cluster** (not in `eval.py`):

```
spark.sql.adaptive.enabled false
spark.sql.autoBroadcastJoinThreshold -1
spark.sql.shuffle.partitions 2000
spark.sql.files.maxPartitionBytes 32768
```

4. `notebooks/eval.py` — unpatched KPI (joins, then USA filter). Measured SQL write **~312s**.
5. Let Genie Code use this App + the skills, then run again. Measured **~92s**.
6. Ceiling, same spoiled tables, session-only, does not rewrite sources: `notebooks/kpi_run_ideal.py` (**~60s** job / **~50s** SQL). It sets AQE, broadcast, `maxPartitionBytes=128m`, `openCostInBytes=0`, `shuffle.partitions=8`, USA filter first, `broadcast` dims, FX `dropDuplicates` on `(currency, rate_date)`.

Widgets default to `hive_metastore.spark_tuning_test.transactions` and `hive_metastore.spark_tuning_work`.
