---
name: spark-debugging
description: Debug failed or pathological Apache Spark and PySpark applications from Spark UI, logs, and the physical plan. Use for task exceptions, USER_RAISED_EXCEPTION, executor loss, driver vs executor OOM, spill vs crash, fetch failures, Python worker deaths, and retries that hide the first error.
---

# Spark debugging

Find the **first** error that explains the rest. Later fetch-failed, timeout, and “executor lost” lines are usually debris. Configuration is intent. Task metrics, plan nodes, and logs are behavior.

## Where to look

Spark UI of the failed app (live driver UI or History Server): Jobs → failed stage → **Tasks** (error message, partition index, attempt, host). Same UI on standalone, YARN, Kubernetes, EMR, Databricks. SQL warehouses: failed query → profile + error.

Also: driver log / cluster events for the first exception (a driver crash can show **zero** failed Spark jobs). **Executors** tab: who died, remove reason, GC. **SQL** tab: `raise_error`, `BatchEvalPython`, the operator that was running when it died.

If UI is not available, ask for the Spark UI / History URL or the error + stage/task screenshot. Do not guess.

## Classify the failure

Walk this list. Stop when one cause explains the chain.

1. **Deterministic code or SQL.** Same partition index, same exception, every attempt: `USER_RAISED_EXCEPTION`, `raise_error(...)`, Python `raise`, a poison record. Retries will not save it. If the business needs to quarantine rows, filter them to a reject output and write the rest; do not plant an error expression in the write plan for every matching row.

2. **Executor OOM, usually one task.** Dead executors / cluster event OOM. Then the dying stage: max input or shuffle-read ≫ median → **skew or a huge build**, not “the cluster is undersized”. Broadcast hash tables sit in storage memory and **cannot spill**. Fix keys/strategy before raising executor memory.

3. **Spill, not a crash.** Stage SUCCEEDED with memory/disk spill. Execution memory filled; Spark wrote sort/hash files and continued. That is a size/partition/skew tuning problem, not an OOM. Do not treat spill counters as a failed job.

4. **Driver death.** No failed jobs, or heartbeats from executors while the app is gone. `collect` / `toPandas` of a large frame, collecting a too-large broadcast, or metadata blow-up from extreme partition counts. Aggregate on the cluster; sample with `take`/`limit`.

5. **Python worker.** Errors mention Python worker; plan has `BatchEvalPython` / `ArrowEvalPython`. Missing pandas/pyarrow on **executors** fails Pandas UDFs even if the driver imported them.

6. **Fetch failed / executor lost (secondary).** The message names a shuffle block and an executor. That executor already died: diagnose *that* death (OOM, preemption, disk). Raising network timeouts buries the cause.

7. **One bad host vs one bad partition.** All failures on one host → infra. All on one partition index, many hosts → data or code. Mix of hosts with later SUCCESS on retry → flaky infra.

8. **Timeouts and speculation.** Heartbeat/RPC timeouts follow GC or a stuck fat task. Speculation duplicates a straggler; if the partition is genuinely huge, the duplicate is huge too. Accumulators re-add on retry; never use them for money or exact counts.

A deterministic `raise_error` will burn every attempt. `spark.task.maxFailures` will not fix it.

## After it stays up

If the job now runs but is still wrong or still huge, switch to performance tuning (plan, shuffle, join strategy). Debugging stops at a stable, explained failure or a confirmed infra flake.

## Report

Confirmed vs hypothesis, first error (message, partition, executor, time), chain of fallout, smallest fix or reproduction, what evidence is still missing. Do not change production settings without approval.
