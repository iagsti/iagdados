from dagster import ScheduleDefinition
from .jobs import pessoas_job

pessoas_schedule = ScheduleDefinition(
    name="pessoas_schedule",
    job=pessoas_job,
    cron_schedule="0 6 * * 1-5",
    execution_timezone="America/Sao_Paulo"
)
