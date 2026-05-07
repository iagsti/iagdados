from dagster import ScheduleDefinition
from .jobs import contratos_job


contratos_schedule = ScheduleDefinition(
    name="contratos_schedule",
    job=contratos_job,
    cron_schedule="0 6 * * 1-5",
    execution_timezone="America/Sao_Paulo"
)
