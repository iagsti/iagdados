from dagster import ScheduleDefinition
from .jobs import lattes_job

lattes_schedule = ScheduleDefinition(
    name="lattes_schedule",
    job=lattes_job,
    cron_schedule="0 6 * * 1-5",
    execution_timezone="America/Sao_Paulo"
)