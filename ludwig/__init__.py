from dagster import (AssetSelection,
                     Definitions,
                     ScheduleDefinition,
                     define_asset_job,
                     load_assets_from_modules,)
from . import assets


all_assets = load_assets_from_modules([assets])

# Define a job that will materialize the assets
ludwig_job = define_asset_job('ludwig_job', selection=AssetSelection.all())

# ScheduleDefinition the job it should run and a cron shecule of how frequently to run it
ludwig_schedule = ScheduleDefinition(job=ludwig_job,
                                     cron_schedule='0 * * * *') # every hour

# Definitions object may be useful to combine different definitions
defs = Definitions(
    assets=all_assets,
    jobs=[ludwig_job],
    schedules=[ludwig_schedule]
)


