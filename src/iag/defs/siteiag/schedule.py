from dagster import ScheduleDefinition
from .jobs import siteiag_job


dashboard_schedule = ScheduleDefinition(
    name="siteiag_schedule",
    job=siteiag_job,
    cron_schedule="0 6 * * 1-5",
    execution_timezone="America/Sao_Paulo"
)
