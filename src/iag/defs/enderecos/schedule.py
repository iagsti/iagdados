from dagster import ScheduleDefinition
from .jobs import enderecos_job

enderecos_schedule = ScheduleDefinition(
    name="enderecos_schedule",
    job=enderecos_job,
    cron_schedule="0 6 * * 1-5",
    execution_timezone="America/Sao_Paulo"
)
