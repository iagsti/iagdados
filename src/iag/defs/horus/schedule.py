from dagster import ScheduleDefinition
from .jobs import horus_job

horus_schedule = ScheduleDefinition(
    name="horus_schedule",
    job=horus_job,
    cron_schedule="0 6 * * 1-5",
    execution_timezone="America/Sao_Paulo"
)
