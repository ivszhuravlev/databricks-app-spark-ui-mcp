---
name: spark-performance-tuning
description: Performance-tune Apache Spark and PySpark jobs from the executed physical plan and Spark UI metrics. Use when a job is too slow or too expensive and the fix should be operators, join strategy, shuffle shape, file layout, or partitioning, not a cluster resize. Covers Exchange, BroadcastHashJoin vs SortMergeJoin, skew, small-file scans, task storms, over-partitioning, row fan-out, predicate pushdown, and Python UDF cost.
---

# Spark performance tuning

Tune from **runtime evidence**, then code. Wall-clock without a plan is not a diagnosis. Adding workers or memory is the last move, not the first.

Spark executes **jobs → stages → tasks**. An action submits a job. A shuffle (`Exchange`) opens a new stage. One partition in that stage is one task. If you cannot name the stage that owns wall time and why, you are not tuning yet.

## Where to look

Use the Spark UI (or equivalent query profile) of **this run**, not a guess from source. Same tabs everywhere the driver UI is exposed: standalone, YARN, Kubernetes, EMR, Databricks job/cluster, History Server.

Read **effective conf** too (`Environment` / `sparkProperties`): `spark.sql.shuffle.partitions`, `autoBroadcastJoinThreshold`, AQE flags. Cluster defaults count even when the notebook sets nothing.

| Tab | Read |
| --- | --- |
| Jobs | One action ≈ one job. Duration, failed jobs. Queue / driver time vs executor time. |
| Stages | Duration, task count vs **executor cores**, shuffle read/write, spill. Open every long stage, not only the first interesting join. |
| Stage → Tasks | Duration / shuffle read / input **min vs median vs max**. Max ≫ median is skew. Many sub-second tasks is a task storm. |
| SQL | Final DAG. `Exchange`, `BroadcastHashJoin` vs `SortMergeJoin`, `FileScan` (**files read, bytes, rows out**, pushed filters, read schema), `BatchEvalPython`. Prefer `isFinalPlan=true` if AQE ran. |
| Executors | Lost executors, GC time, shuffle metrics, total cores. |

SQL warehouses often show the same stories as a **query profile** (scans, joins, shuffles, rows) instead of the classic Spark UI.

If you cannot open UI, ask for the Spark UI / History / query-profile URL or a screenshot of Stages + SQL. Do not invent metrics.

**Do not edit code, hints, or Spark conf until you have numbers from this run's UI or query profile** (wall-owning stage, files or task count, join type, shuffle). Source review is not a substitute. If the UI is closed, get it opened. Then diagnose.

## Diagnose first: do not patch yet

Collect evidence for **every** candidate below on this run. Rule each in or out with numbers. **Do not settle for the first plausible explanation** (a `SortMergeJoin` on a tiny dimension is not automatically the reason the job took minutes).

Then name **one** stage (or SQL node) that owns wall time. The fix must change *that* stage. A correct join rewrite on a ten-minute scan does not make the job fast.

An `Exchange` is not automatically waste. `SortMergeJoin` is not automatically wrong. `BroadcastHashJoin` is not automatically safe or free.

## Candidates (checklist, not a patch order)

**Scan / small files.** Long `FileScan` (or the stage that runs it): `files read` or task count in the thousands to tens of thousands, rows per file tiny, bytes read ≫ rows you keep. Missing partition or pushed filters, or the table was written as many small files. Coalesce / rewrite / larger `maxPartitionBytes` / fewer output files, before you broadcast into that scan.

**Task storm.** Stage `numTasks` ≫ cores (thousands of short tasks). Scheduler and launch overhead. Typical sources: `shuffle.partitions` in the thousands, `repartition(N)` on small data, one task per tiny file. Compare `numTasks` to total executor cores.

**Invented work / fan-out.** `explode` / `array_repeat` / unrestricted joins that multiply rows *before* an aggregate or shuffle. If the result grain is the original table, the fan-out is a bug. Compare scan rows to rows into the first `Exchange`.

**Join grain.** Join output ≫ both inputs → many-to-many (key too coarse). Join the key that matches the business grain, or pre-aggregate.

**Join strategy.** From **observed** size, not table names.
- Tiny build side + `SortMergeJoin` → consider `broadcast(df)`, or stop forcing `autoBroadcastJoinThreshold=-1`.
- In-memory lookups (`createDataFrame`) often have unknown size → planner chooses SMJ.
- Two large facts → SMJ (or SHJ) is correct. Do not broadcast tens of gigabytes.
- Broadcast lives in **storage** memory and **cannot spill**; it fits or it OOMs.
- Broadcasting the small side **into a tiny-file / high-task probe scan** can make wall time worse: every split still runs, now with a BHJ. Fix the scan/task storm first if that stage owns the clock.

**Pointless shuffles.** `repartition(N)` with N in the hundreds/thousands on small data. `repartition` always shuffles; `coalesce` does not. Keep `repartition(n, key)` for key alignment only. AQE (`AQEShuffleRead`) can merge tiny partitions **after** you already paid to create them.

**Skew.** Max shuffle-read or runtime many times the median → hot key. Prefer a real key or broadcast the small side *if the probe side is not a file storm*. Salt only when both sides are large. Speculation copies the fat partition. AQE skew split only applies to **SMJ after shuffle**.

**Wide rows / spill.** Drop columns before the wide op. Partial aggregate before join when the logic allows. Spill with SUCCESS means execution memory filled: reduce state or raise partition count **after** skew and task-storm are ruled out.

**Python.** `BatchEvalPython` is a codegen barrier. Prefer SQL / built-ins; Pandas/Arrow UDFs next. Executors must have the packages you call.

**AQE.** Coalesce tiny shuffle parts, switch SMJ→BHJ when *runtime* size is small, split oversized SMJ partitions. It will not delete a useless explode, a wrong join key, a Python UDF, or a 20k-file table.

**Cluster resize last.** More workers help when tasks are busy, not skewed, and parallelism is actually capped by cores. More heap helps a uniformly fat partition, not a 100× hot key.

## What “fast” looks like

- Task count on a long stage is on the order of **cores**, not thousands, unless the data really needs it.
- Median task time close to max (no single partition owning the stage).
- Scans: pushed filters, narrow schema, files not orders of magnitude above partitions you need.
- Join with a small dimension is BHJ **unless** the probe scan is already the wall.
- Shuffle bytes are justified by the **keys you actually need**.

## Report

Highest-impact change tied to the wall-owning stage, plan node + stage id, numbers (files, rows, shuffle bytes, tasks vs cores, median vs max). Candidates you rejected and why. Correctness risk (join keys, duplicates, ordering). No patch without those UI numbers. Do not apply production conf or expensive reruns without approval.
