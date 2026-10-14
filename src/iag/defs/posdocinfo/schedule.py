from dagster import ScheduleDefinition
from .jobs import posdoc_job

posdoc_schedule = ScheduleDefinition(
    name="posdoc_schedule",
    job=posdoc_job,
    cron_schedule="0 6 * * 1-5",
    execution_timezone="America/Sao_Paulo",
)
