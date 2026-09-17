from dagster import ScheduleDefinition
from .jobs import alunosic_job

alunosic_schedule = ScheduleDefinition(
    name="alunosic_schedule",
    job=alunosic_job,
    cron_schedule="0 6 * * 1-5",
    execution_timezone="America/Sao_Paulo",
)
