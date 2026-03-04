import dagster as dg
import geopandas as gpd
import matplotlib.pyplot as plt
import pandas as pd
from dagster_duckdb import DuckDBResource

from dagster_essentials.defs.assets import constants, trips
from dagster_essentials.defs.partitions import weekly_partition


@dg.asset(
    deps=[
        trips.taxi_trips,
        trips.taxi_zones,
    ],
    group_name="metrics",
)
def manhattan_stats(database: DuckDBResource) -> None:
    query = """
        select
            z.zone,
            z.borough,
            z.geometry,
            count(1) as num_trips
        from
            trips t
            left join zones z on t.pickup_zone_id = z.zone_id
        where
            upper(z.borough) = 'MANHATTAN'
            and z.geometry is not null
        group by
            z.zone,
            z.borough,
            z.geometry
    """

    with database.get_connection() as conn:
        trips_by_zone = conn.execute(query).fetch_df()

    trips_by_zone["geometry"] = gpd.GeoSeries.from_wkt(data=trips_by_zone["geometry"])
    trips_by_zone = gpd.GeoDataFrame(data=trips_by_zone)

    with open(constants.MANHATTAN_STATS_FILE_PATH, "w") as output_file:
        output_file.write(trips_by_zone.to_json())


@dg.asset(
    deps=[
        manhattan_stats,
    ],
    group_name="metrics",
)
def manhattan_map() -> None:
    trips_by_zone = gpd.read_file(constants.MANHATTAN_STATS_FILE_PATH)

    figure, axes = plt.subplots(figsize=(10, 10))

    trips_by_zone.plot(
        column="num_trips",
        cmap="plasma",
        legend=True,
        ax=axes,
        edgecolor="black",
    )

    axes.set_title("Number of Trips per Taxi Zone in Manhattan")
    axes.set_xlim(-74.05, -73.90)  # TODO what do these numbers represent?
    axes.set_ylim(40.70, 40.82)  # TODO what do these numbers represent?

    plt.savefig(constants.MANHATTAN_MAP_FILE_PATH, format="png", bbox_inches="tight")
    plt.close(figure)


@dg.asset(
    deps=[
        trips.taxi_trips,
    ],
    group_name="metrics",
    partitions_def=weekly_partition,
)
def trips_by_week(context: dg.AssetExecutionContext, database: DuckDBResource) -> None:
    week_date = context.partition_key

    query = f"""
                select
                    vendor_id, total_amount, trip_distance, passenger_count
                from trips
                where date_trunc('week', pickup_datetime) = date_trunc('week', '{week_date}'::date)
            """

    with database.get_connection() as conn:
        data_for_week = conn.execute(query).fetch_df()

    aggregate = (
        data_for_week.agg(
            {
                "vendor_id": "count",
                "total_amount": "sum",
                "trip_distance": "sum",
                "passenger_count": "sum",
            }
        )
        .rename({"vendor_id": "num_trips"})
        .to_frame()
        .T
    )  # type: ignore

    aggregate["period"] = week_date

    # clean up the formatting of the dataframe
    aggregate["num_trips"] = aggregate["num_trips"].astype(int)
    aggregate["passenger_count"] = aggregate["passenger_count"].astype(int)
    aggregate["total_amount"] = aggregate["total_amount"].round(2).astype(float)
    aggregate["trip_distance"] = aggregate["trip_distance"].round(2).astype(float)

    aggregate = aggregate[
        [
            "period",
            "num_trips",
            "total_amount",
            "trip_distance",
            "passenger_count",
        ]
    ]

    try:
        existing = pd.read_csv(filepath_or_buffer=constants.TRIPS_BY_WEEK_FILE_PATH)
        existing = existing[existing["period"] != week_date]
        existing = pd.concat([existing, aggregate]).sort_values(by="period")
        existing.to_csv(path_or_buf=constants.TRIPS_BY_WEEK_FILE_PATH, index=False)

    # TODO would prefer an explicit check instead of catching an exception, there are other reasons
    # this might fail.
    except FileNotFoundError:
        aggregate.to_csv(path_or_buf=constants.TRIPS_BY_WEEK_FILE_PATH, index=False)
