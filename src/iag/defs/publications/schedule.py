from dagster import ScheduleDefinition
from .jobs import publications_job

publications_schedule = ScheduleDefinition(
    name="publications_schedule",
    job=publications_job,
    cron_schedule="0 6 * * 1-5",
    execution_timezone="America/Sao_Paulo"
)