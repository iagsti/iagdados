from dagster import ScheduleDefinition
from .jobs import dashboard_job


dashboard_schedule = ScheduleDefinition(
    name="dashboard_schedule",
    job=dashboard_job,
    cron_schedule="0 6 * * 1-5",
    execution_timezone="America/Sao_Paulo"
)
