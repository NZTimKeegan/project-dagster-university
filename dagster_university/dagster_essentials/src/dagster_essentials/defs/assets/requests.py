import base64

import dagster as dg
from dagster_duckdb import DuckDBResource

import matplotlib.pyplot as plt

from dagster_essentials.defs.assets import constants, trips


class AdhocRequestConfig(dg.Config):
    filename: str
    borough: str
    start_date: str
    end_date: str


@dg.asset(
    deps=[
        trips.taxi_trips,
        trips.taxi_zones,
    ],
    group_name="requests",
)
def adhoc_request(config: AdhocRequestConfig, database: DuckDBResource) -> dg.MaterializeResult:
    # TODO I would prefer to use the `Path` object here to do the manipulation, including
    # replacing the templated file path, so we can use `pathlib` operations to safely create files
    request_file_name = config.filename.split(".")[0]

    output_file_path = constants.REQUEST_DESTINATION_TEMPLATE_FILE_PATH.format(
        request_file_name
    )

    query = f"""
        select
            date_part('hour', t.pickup_datetime) as hour_of_day,
            date_part('dayofweek', t.pickup_datetime) as day_of_week_num,
            case date_part('dayofweek', t.pickup_datetime)
                when 0 then 'Sunday'
                when 1 then 'Monday'
                when 2 then 'Tuesday'
                when 3 then 'Wednesday'
                when 4 then 'Thursday'
                when 5 then 'Friday'
                when 6 then 'Saturday'
            end as day_of_week,
            count(1) as num_trips
        from
            trips t
            left join zones z on t.pickup_zone_id = z.zone_id
        where
            t.pickup_datetime >= '{config.start_date}'
            and t.pickup_datetime < '{config.end_date}'
            and z.borough = '{config.borough}'
        group by 1, 2
        order by 1, 2 asc
    """

    with database.get_connection() as conn:
        results = conn.execute(query).fetch_df()

    figure, axes = plt.subplots(figsize=(10, 6))

    results_pivot = results.pivot(
        index="hour_of_day", columns="day_of_week", values="num_trips"
    )
    results_pivot.plot(kind="bar", stacked=True, ax=axes, colormap="viridis")

    axes.set_title(
        f"Number of trips by hour of day in {config.borough}, from {config.start_date} to {config.end_date}"
    )

    axes.set_xlabel("Hour of Day")
    axes.set_ylabel("Number of Trips")
    axes.legend(title="Day of Week")

    plt.xticks(rotation=45)
    plt.tight_layout()

    plt.savefig(output_file_path)
    plt.close(figure)

    with open(file=output_file_path, mode="rb") as file:
        image_bytes = file.read()

    image_bytes_base64 = base64.b64encode(image_bytes).decode()
    markdown_image_link = f"![Image](data:image/jpeg;base64,{image_bytes_base64})"

    return dg.MaterializeResult(
        metadata={
            "preview": dg.MetadataValue.md(data=markdown_image_link),
        }
    )
