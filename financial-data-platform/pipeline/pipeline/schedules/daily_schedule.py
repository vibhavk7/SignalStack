"""Daily orchestration schedule for all platform assets."""

from __future__ import annotations

from dagster import AssetSelection, DefaultScheduleStatus, define_asset_job, schedule

full_pipeline_job = define_asset_job("full_pipeline_job", selection=AssetSelection.all())


@schedule(
    cron_schedule="0 6 * * *",
    job=full_pipeline_job,
    execution_timezone="UTC",
    default_status=DefaultScheduleStatus.RUNNING,
)
def daily_full_pipeline_schedule() -> dict[str, object]:
    """Run the full Bronze, Silver, and Gold pipeline every day at 06:00 UTC."""

    return {}
