from dagster import ScheduleDefinition
from .jobs import pessoasinfo_job

pessoasinfo_schedule = ScheduleDefinition(
    name="pessoasinfo_schedule",
    job=pessoasinfo_job,
    cron_schedule="0 6 * * 1-5",
    execution_timezone="America/Sao_Paulo",
)
