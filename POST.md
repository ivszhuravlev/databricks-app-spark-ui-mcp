# Draft — first person

I use Genie Code like everyone else for coding, debugging, and tuning Spark pipelines. I did not have capacity to sit in Spark UI looking for skew and task storms, so I asked it to do that.

Genie Code cannot natively open classic compute Spark UI. The MCP catalog has no Spark UI server. I did not find a ready Azure Databricks option. If you have one, please comment.

I sketched a small Databricks App that serves MCP and reads Spark UI on a classic cluster through the driver-proxy API (Spark UI REST, port 40001). You pass `cluster_id`. Tools are read-only.

I also keep portable Spark performance-tuning skills. Alone they were useless here: still no UI. Skills plus this App, on a spoiled fake KPI eval: **~312s → ~92s** (~70% faster). Same tables, notebook only. I think that is a decent result, so I am sharing the repo.

The job is eval, not production. Repo has prepare / spoil / eval / `kpi_run_ideal`. Human ceiling on the same spoiled tables, session-only (including FX dedup): **~50–60s**.

---

Как все, пользуюсь Genie Code для пайплайнов. Не было времени сидеть в Spark UI — попросил агента. Нативно классический Spark UI он не открывает, готового решения под Azure Databricks я не нашёл (если есть — напишите).

Собрал небольшой Databricks App MCP: Spark UI на classic compute через driver-proxy. Отдельные skills без UI почти бесполезны. Вместе, на испорченном eval KPI: **~312с → ~92с** (около −70%). Потолок на тех же таблицах: **~50–60с**. Это фейковый KPI. В репо: prepare, spoil, eval, ideal.
