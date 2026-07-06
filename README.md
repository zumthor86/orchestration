# orchestration

The thin cross-project **conductor** for the daily quant pipeline (feature `003-ingest-core-orchestration`).

## What lives here

| File | Purpose |
|---|---|
| `daily_pipeline.py` | The `daily-quant-pipeline` Prefect flow — the **only scheduled** entry point. Triggers the four stage deployments strictly in order, gating each on the prior's `Completed` state. |
| `notify.py` | Failure notification to the configured channel (`PIPELINE_ALERT_CHANNEL`). Exactly one notification per failed run; none on success. |
| `prefect.yaml` | Prefect deployment definition for the conductor (carries the daily schedule — the only one in the pipeline). |

## Hard rules

- **Imports NO app code.** Stages are referenced by deployment *name-string* only via `run_deployment(...)` — no `import hermes`, no `import processing`. This keeps the dependency graph acyclic (research D1).
- **`as_subflow=True`** (the default) links each stage run as a child of the conductor run, so the whole pipeline is visible in a single run view (FR-014). Execution still happens on each stage's own work pool/venv.
- Stage retries live *inside* the stage deployments — the conductor observes final outcomes only (D10).

## Stage order (FR-008)

```
hermes-ingest  →  hephaestus-ingest  →  hephaestus-surface  →  hermes-screen
```

## Environment

```bash
PREFECT_API_URL=http://127.0.0.1:4200/api   # shared Prefect server
PIPELINE_ALERT_CHANNEL=...                  # notification target (FR-019); consumed HERE, not by the apps
```

## Setup

```bash
python -m venv .venv
.venv/Scripts/pip install -r requirements.txt
prefect deploy    # registers daily-quant-pipeline from deployments.yaml / prefect.yaml
```
