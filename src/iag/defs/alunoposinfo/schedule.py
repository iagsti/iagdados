from dagster import ScheduleDefinition
from .jobs import alunosposinfo_job

alunosposinfo_schedule = ScheduleDefinition(
    name="alunosposinfo_schedule",
    job=alunosposinfo_job,
    cron_schedule="0 6 * * 1-5",
    execution_timezone="America/Sao_Paulo",
)
