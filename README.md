# Spark UI for Genie Code

A Databricks App (MCP) and two Genie Code skills. Install both. Skills without the App guess from source. The App without the skills is raw Spark UI JSON.

## Why

Genie Code can read a notebook. It cannot open classic Spark UI: Jobs, Stages, SQL, Environment, tasks.

The [Genie Code MCP catalog](https://docs.databricks.com/aws/en/genie-code/mcp) has no Spark UI server. A skill that says "read Stages" does nothing if there is no tool. A tool that dumps JSON does not tell the agent which tab owns wall time.

This repo is the pair: an App that reads the live classic driver, and two skills that say how to read it.

## Repo

| Path | Role |
| --- | --- |
| [`apps/mcp-spark-ui`](apps/mcp-spark-ui) | Databricks App. MCP at `/mcp`. Reads Spark UI through driver-proxy. |
| [`skills/spark-performance-tuning`](skills/spark-performance-tuning/SKILL.md) | Slow or expensive job: wall stage, files, joins, shuffle, Environment. |
| [`skills/spark-debugging`](skills/spark-debugging/SKILL.md) | Failed or pathological job: first error, OOM vs spill, driver vs executor. |
| [`notebooks/`](notebooks) | Optional planted KPI. Not part of install. |

Works on a **Running classic** cluster. Not serverless, not SQL warehouse query profile, not History Server. After a cluster restart the old Spark app is gone.

## Install

Need permission to create Databricks Apps.

### 1. App

1. Put `apps/mcp-spark-ui` in the workspace you will debug.
2. Create a Databricks App from that folder. Start command is in `app.yaml`: `uv run custom-mcp-server`.
3. On the App page, copy the **app service principal**. Grant it `CAN_ATTACH_TO` on each classic cluster you will inspect. The App talks to the driver as that principal.
4. Grant teammates `CAN_USE` on the App.
5. Apps inject `DATABRICKS_HOST`. Azure `adb-<org>.…` hosts already carry the org id. On AWS or GCP set `SPARK_UI_ORG_ID` (the `o=` value in the workspace URL). If the workspace URL and the App origin differ, set `SPARK_UI_CORS_ORIGINS` (comma-separated).
6. MCP URL: `https://<app-url>/mcp`.
7. Genie Code → Settings → MCP servers → add this App. Turn on every `spark_ui_*` tool, not only `health`. Use **Agent** mode. A workspace admin can disable custom MCP.

Pass `cluster_id` from **Compute**. Do not pass `spark_context_id` from the Spark UI URL.

Tools: `health`, `spark_ui_applications`, `spark_ui_sql`, `spark_ui_environment`, `spark_ui_executors`, `spark_ui_job`, `spark_ui_stage`, `spark_ui_task_list`. See [custom MCP](https://docs.databricks.com/aws/en/agents/mcp-tools/custom-mcp).

### 2. Skills

Same install. Copy the folders, do not "attach later".

Just you:

```
/Workspace/Users/<you>/.assistant/skills/spark-performance-tuning/SKILL.md
/Workspace/Users/<you>/.assistant/skills/spark-debugging/SKILL.md
```

Whole workspace (admin):

```
/Workspace/.assistant/skills/spark-performance-tuning/SKILL.md
/Workspace/.assistant/skills/spark-debugging/SKILL.md
```

Or Genie Code → Settings → Open skills folder, then put both folders there. [Docs](https://docs.databricks.com/aws/en/genie-code/skills).

Open a **new** Agent chat. If a skill does not load, `@` it.

## Use it on your job

Cluster **Running**. In Agent mode give the Compute `cluster_id` and the run.

1. `spark_ui_applications` for the live `application_id`
2. `spark_ui_sql` for the SQL that owns wall time
3. `spark_ui_environment` for effective conf (cluster defaults count)
4. `spark_ui_job` / `spark_ui_stage` / `spark_ui_task_list` for files, rows, join type, skew

## Check the install

On your job. You do not need the notebooks below.

- Browser: `GET https://<app-url>/health` → `{"status":"healthy"}`
- `/mcp` `tools/list` lists `spark_ui_*`
- `spark_ui_applications` with a Compute `cluster_id` returns the current app

`tools/list` works but applications or SQL return 403: the app service principal cannot reach that cluster.

---

## Optional: planted KPI

Only if you want a slow job in this repo. Skip on a real project.

1. `notebooks/prepare_data.py` builds clean work tables.
2. `notebooks/spoil_data.py` plants tiny files, store-key skew, and multi-vendor FX.
3. Set on the **cluster**, not in `eval.py`:

```
spark.sql.adaptive.enabled false
spark.sql.autoBroadcastJoinThreshold -1
spark.sql.shuffle.partitions 2000
spark.sql.files.maxPartitionBytes 32768
```

4. `notebooks/eval.py` is the unpatched pipeline.
5. `notebooks/kpi_run_ideal.py` is a session-only ceiling on the same spoiled tables. It does not rewrite sources.

Widgets: `hive_metastore.spark_tuning_test.transactions`, `hive_metastore.spark_tuning_work`.
