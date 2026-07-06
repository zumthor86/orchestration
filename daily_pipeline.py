"""Conductor flow for the daily quant pipeline (feature 003).

Owns the ONLY schedule (FR-010) and sequences the four stage deployments
strictly in order, gating each on the prior's ``Completed`` state (FR-008/9/11):

    hermes-ingest -> hephaestus-ingest -> hephaestus-surface -> hermes-screen

Hard rules (research D1/D2):
- Imports NO app code — stages are referenced by deployment name-string only,
  so neither app ever depends on the other and no import cycle can form.
- ``as_subflow=True`` links each stage run as a child of this run, giving the
  single consolidated run view (FR-014). Execution still happens on each
  stage's own work pool / venv (FR-015) — linkage is metadata only.
- Stage retries live inside the stage deployments (D10); this flow observes
  final outcomes only and carries ``retries=0``.
"""
from __future__ import annotations

import os
from datetime import date
from typing import Optional

from dotenv import load_dotenv
from prefect import flow, get_run_logger
from prefect.artifacts import create_table_artifact
from prefect.deployments import run_deployment

from notify import COVERAGE_MARKER, send_notification

load_dotenv()

# "<flow-name>/<deployment-name>" name-strings — the whole cross-project contract.
STAGE_ORDER = [
    "hermes-ingest/hermes-ingest",
    "hephaestus-ingest/hephaestus-ingest",
    "hephaestus-surface/hephaestus-surface",
    "hermes-screen/hermes-screen",
]


def _run_ui_url(flow_run_id: str) -> Optional[str]:
    api = os.getenv("PREFECT_API_URL", "")
    if not api:
        return None
    return api.replace("/api", "") + f"/flow-runs/flow-run/{flow_run_id}"


def _publish_run_summary(target_date, results: list[dict]) -> None:
    """Run-level summary artifact aggregating per-stage outcomes (contracts/run-artifacts.md, MAY)."""
    try:
        create_table_artifact(
            key=f"pipeline-summary-{target_date or 'latest'}".lower(),
            table=results,
            description=f"daily-quant-pipeline stage outcomes ({target_date or 'latest session'})",
        )
    except Exception:  # pragma: no cover — a summary failure must not mask the run outcome
        pass


@flow(name="daily-quant-pipeline", retries=0, log_prints=True)
def daily_pipeline(target_date: Optional[date] = None) -> None:
    """Run the four stages in order; stop the line and notify on first failure."""
    logger = get_run_logger()
    results: list[dict] = []

    for name in STAGE_ORDER:
        logger.info("Triggering stage %s ...", name)
        run = run_deployment(
            name=name,
            parameters={"target_date": target_date},
            as_subflow=True,  # child linkage -> single run view (FR-014); executes on its own pool
        )
        state = run.state
        state_name = state.name if state is not None else "Unknown"
        results.append({"stage": name, "state": state_name, "run_id": str(run.id)})
        if state is not None and state.is_completed():
            logger.info("Stage %s Completed.", name)
            continue

        # FR-011: stop the line. FR-019/D9: exactly one notification, sent here.
        message = (state.message or "") if state is not None else ""
        trigger = "coverage_below_floor" if COVERAGE_MARKER in message else "stage_failed"
        summary = f"{name} ended {state_name}: {message or 'no state message'}"
        logger.error("Pipeline stopped — %s", summary)
        for skipped in STAGE_ORDER[STAGE_ORDER.index(name) + 1:]:
            results.append({"stage": skipped, "state": "NotRun (upstream failed)", "run_id": ""})
        _publish_run_summary(target_date, results)
        send_notification(
            run_id=str(run.id),
            target_date=str(target_date or "latest"),
            trigger=trigger,
            failing_stage=name,
            summary=summary,
            run_url=_run_ui_url(str(run.id)),
        )
        raise RuntimeError(f"daily-quant-pipeline stopped: {summary}")

    _publish_run_summary(target_date, results)

    # Success path: all four stages Completed -> NO notification (FR-019/SC-011).
    logger.info(
        "daily-quant-pipeline: all four stages Completed for %s.",
        target_date or "latest session",
    )


if __name__ == "__main__":
    daily_pipeline()
