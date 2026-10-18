# P7D — Current-release FLINK-39965 integration reproduction

**Status: run-ready *candidate* (static checks PASS); full Docker/Flink/MySQL/StarRocks execution NOT performed here.**

## Scientific question
Can *unmodified* Apache Flink CDC 3.7.0 reproduce the reported `DROP TABLE → CREATE TABLE with same TableId → INSERT` schema-cache failure under a real MySQL → Flink CDC → StarRocks pipeline, with or without one TaskManager restart?

This is **not** a new algorithm and not evidence of a Q1 novelty claim. The public proposed patch already targets the schema cache issue; see PR #4448. A later patched-vs-unpatched study would require a separately validated custom build.

## What will actually run
- Independent short-lived Docker environment for each GitHub Actions matrix job: MySQL 8.0, StarRocks 3.5.10, Apache Flink 1.20.0, Apache Flink CDC 3.7.0-1.20 and the matching Maven connectors.
- The exact checkpoint system is real Flink (not a simulated state machine).
- Before Flink starts, MySQL contains two similar tables with one row each, `flink.orders_0` and `flink.orders_1`, so the initial snapshot captures both.
- Negative control: add row id=2 to **unchanged** `orders_1` and require that it reach StarRocks.
- Positive issue case: wait until the initial `orders_0` row reaches StarRocks, DROP the source table, wait until the StarRocks table is observed absent, RECREATE under the same name and insert id=2; poll StarRocks for the second row.
- Restart mode: after the sink-side DROP is observed, require at least one completed checkpoint, restart the TaskManager, require the Flink job to return to RUNNING, then RECREATE and INSERT.
- Each matrix job has fresh container storage and a fresh job; 1 or 10 cold-start repetitions per mode.
- The result captures per-stage status, Flink job state, Docker Compose logs and checksums.

## Run WITHOUT owning an Ubuntu computer
This requires a GitHub account with Actions permitted (and an available hosted Linux runner). **A public repository is recommended for the 16-GB hosted runner; it makes uploaded files/logs potentially publicly visible. Do not include real credentials or private data.** Resource/download availability may still block execution.

1. Download and unzip `P7D_FlinkCDC37_GitHubActions_Kit.zip` on your computer.
2. Create a NEW empty GitHub repository, e.g. `p7d-flink-cdc-repro`. Do not use production credentials.
3. Upload the extracted `docker-compose.yml`, `scripts/` folder, and `.github/workflows/p7d-real-integration.yml` **preserving their paths**. For the hidden `.github` directory, GitHub's web interface also lets you use **Add file → Create new file** and enter the full path `.github/workflows/p7d-real-integration.yml`, then paste its contents.
4. On the repository's **Actions** tab select `P7D Flink CDC 3.7.0 MySQL-StarRocks reproduction` → **Run workflow**. Choose `1` first. (You will get one normal and one restart cold-start job.)
5. Inspect both runs. Under their **Artifacts** sections download `P7D-CDC37-normal-1.zip` and `P7D-CDC37-restart-1.zip`. Upload **both** to the ChatGPT conversation for analysis, including failed setup runs.
6. **Only after** both runs establish working infrastructure may you choose `10` for a full 10+10 independent repetitions. Otherwise, changing versions or tuning during the gate would undermine comparability.

The GitHub workflow does not automatically run on a push; you must start it manually. On some accounts, Actions require enabling by a repository administrator. Full-run resource costs and startup time are not guaranteed, especially for private repositories.

## Evidence classification — DO NOT conflate
- `CORRECT_RECREATION`: source/sink sequence completed and row id 2 was found. Does not imply every CDC path is correct.
- `SUSPECT_CDC_LIFECYCLE_FAILURE_REQUIRES_LOG_TRIAGE`: initial sink row and sink DROP observed, source RECREATE+INSERT succeeded, but final row was missing. Confirm Flink logs contain *redundant CreateTableEvent* and/or *unknown table*, and check the control.
- `INCONCLUSIVE_OR_SETUP_FAILURE`: source, sink, checkpoint, or task restart was not demonstrably healthy. **Not a reproduced bug.**
- `CONTROL_FAILED_OR_SETUP`: ordinary non-recreation CDC behavior failed. Do not attribute a target failure to schema-reuse logic until the control passes.

## Environment limitations and current state
- Local analysis environment: Java available, no Docker/Maven/MySQL/StarRocks and no direct downloads. Therefore **no new full integration reproduction has been performed**. Source and YAML references were checked, not a live pipeline.
- Flink CDC 3.7.0 actual downloads are in `https://dlcdn.apache.org/flink/flink-cdc-3.7.0/`; use CDC `-1.20` distribution with matching Maven connectors, NOT the `-2.2` binaries.
- Connector artifact resolution, Flink container image availability, StarRocks startup, and source/sink compatibility have NOT been tested end-to-end.
- The Docker Compose deployment uses local **test-only credentials**. Do not connect it to a real database.
- For the primary comparison we **do not** patch CDC; public PR #4448 is still open in the reviewed source. We will examine it after baseline reproduction and ensure equivalent configurations.

## Literature and issue references
- Official FLINK-39965: https://issues.apache.org/jira/browse/FLINK-39965
- Existing proposed repair: https://github.com/apache/flink-cdc/pull/4448
- CDC release 3.7.0: https://dlcdn.apache.org/flink/flink-cdc-3.7.0/
- CDC pipeline schema behavior: https://nightlies.apache.org/flink/flink-cdc-docs-release-3.7/docs/core-concept/data-pipeline/
- Apache Flink CDC MySQL→StarRocks guide: https://nightlies.apache.org/flink/flink-cdc-docs-master/docs/get-started/quickstart-for-2.2/mysql-to-starrocks/
- GitHub-hosted runner resources: https://docs.github.com/en/actions/reference/runners/github-hosted-runners
