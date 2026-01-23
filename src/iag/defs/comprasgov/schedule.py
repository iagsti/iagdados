from dagster import ScheduleDefinition
from .jobs import comprasgov_job

compras_gov_schedule = ScheduleDefinition(
    name="compras_gov_schedule",
    job=comprasgov_job,
    cron_schedule="0 6 * * 1-5",
    execution_timezone="America/Sao_Paulo"
)
