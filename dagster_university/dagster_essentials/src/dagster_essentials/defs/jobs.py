import dagster as dg

from dagster_essentials.defs.assets import metrics, requests
from dagster_essentials.defs.partitions import monthly_partition, weekly_partition

adhoc_request = dg.AssetSelection.assets(requests.adhoc_request)

adhoc_request_job = dg.define_asset_job(
    name="adhoc_request_job",
    selection=adhoc_request,
)

trips_by_week = dg.AssetSelection.assets(metrics.trips_by_week)

weekly_update_job = dg.define_asset_job(
    name="weekly_update_job",
    partitions_def=weekly_partition,
    selection=trips_by_week,
)

trip_update_job = dg.define_asset_job(
    name="trip_update_job",
    partitions_def=monthly_partition,
    selection=dg.AssetSelection.all() - adhoc_request - trips_by_week,
)
