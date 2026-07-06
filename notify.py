"""Failure notification for the daily quant pipeline (FR-019 / research D9).

Sent by the conductor ONLY — exactly one notification per failed run, none on
success. The channel is env-configured (``PIPELINE_ALERT_CHANNEL``), never
hard-coded:

- ``log:``          log-only mode (bring-up default)
- ``https://...``   webhook POST with a Slack/Discord-compatible JSON payload
"""
from __future__ import annotations

import logging
import os
from typing import Optional

import requests

logger = logging.getLogger("daily-quant-pipeline.notify")

# Stage flows embed this marker in the exception message when an ingest domain
# falls below the coverage floor (FR-017), so the conductor can classify the
# notification trigger without importing any app code.
COVERAGE_MARKER = "COVERAGE_BELOW_FLOOR"


def send_notification(
    *,
    run_id: str,
    target_date: str,
    trigger: str,
    failing_stage: str,
    summary: str,
    run_url: Optional[str] = None,
) -> None:
    """Deliver one failure/degraded notification to the configured channel.

    Never raises — a broken notification channel must not mask the underlying
    pipeline failure.
    """
    channel = os.getenv("PIPELINE_ALERT_CHANNEL", "log:")
    text = (
        f"[daily-quant-pipeline] {trigger}: {failing_stage} — {summary} "
        f"(run {run_id}, target_date {target_date}"
        + (f", {run_url}" if run_url else "")
        + ")"
    )
    payload = {
        "text": text,  # Slack-compatible
        "content": text,  # Discord-compatible
        "run_id": run_id,
        "target_date": target_date,
        "trigger": trigger,
        "failing_stage": failing_stage,
        "summary": summary,
        "run_url": run_url,
    }

    if not channel or channel.startswith("log:"):
        logger.error("PIPELINE ALERT (log-only channel): %s", text)
        print(f"PIPELINE ALERT (log-only channel): {text}")
        return

    try:
        resp = requests.post(channel, json=payload, timeout=(5, 15))
        if resp.status_code >= 400:
            logger.error(
                "Notification POST to channel failed (HTTP %s): %s",
                resp.status_code, resp.text[:200],
            )
    except Exception as e:  # pragma: no cover
        logger.error("Notification POST to channel raised: %s — alert text was: %s", e, text)
