import requests

import dagster as dg
import pandas as pd
from dagster_duckdb import DuckDBResource

from dagster_essentials.defs.assets import constants
from dagster_essentials.defs.partitions import monthly_partition


@dg.asset(
    group_name="raw_files",
    partitions_def=monthly_partition,
)
def taxi_trips_file(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """
    The raw parquet files for the taxi trips dataset. Sourced from the NYC Open Data portal.
    """
    # TODO this should convert to a date object back to a string using a format method
    partition_date = context.partition_key
    month_to_fetch = partition_date[:-3]

    raw_trips = requests.get(
        f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{month_to_fetch}.parquet"
    )

    output_file_path = constants.TAXI_TRIPS_TEMPLATE_FILE_PATH.format(month_to_fetch)

    with open(output_file_path, "wb") as output_file:
        output_file.write(raw_trips.content)

    number_of_rows = len(pd.read_parquet(path=output_file_path))

    return dg.MaterializeResult(
        metadata={"number_of_records": dg.MetadataValue.int(number_of_rows)}
    )


@dg.asset(
    group_name="raw_files",
)
def taxi_zones_file() -> dg.MaterializeResult:
    """
    The raw csv file for the taxi zones dataset. Sourced from the NYC Open Data portal.
    """
    raw_zones = requests.get(
        "https://community-engineering-artifacts.s3.us-west-2.amazonaws.com/dagster-university/data/taxi_zones.csv"
    )

    output_file_path = constants.TAXI_ZONES_FILE_PATH

    with open(output_file_path, "wb") as output_file:
        output_file.write(raw_zones.content)

    number_of_rows = len(pd.read_csv(filepath_or_buffer=output_file_path))

    return dg.MaterializeResult(
        metadata={"number_of_records": dg.MetadataValue.int(number_of_rows)}
    )


@dg.asset(
    deps=[
        taxi_trips_file,
    ],
    group_name="ingested",
    partitions_def=monthly_partition,
)
def taxi_trips(
    context: dg.AssetExecutionContext,
    database: DuckDBResource,
) -> None:
    """
    The raw taxi trips dataset, loaded into a DuckDB database. Performs a delete on all records
    that match the incoming partition key before loading to prevent duplicates.
    """
    partition_date = context.partition_key
    month_to_load = partition_date[:-3]

    query = f"""
    create table if not exists trips (
        vendor_id integer,
        pickup_zone_id integer,
        dropoff_zone_id integer,
        rate_code_id double,
        payment_type integer,
        dropoff_datetime timestamp,
        pickup_datetime timestamp,
        trip_distance double,
        passenger_count double,
        total_amount double,
        partition_date varchar
    );

    delete from trips where partition_date = '{month_to_load}';

    insert into trips (
        vendor_id,
        pickup_zone_id,
        dropoff_zone_id,
        rate_code_id,
        payment_type,
        dropoff_datetime,
        pickup_datetime,
        trip_distance,
        passenger_count,
        total_amount,
        partition_date
    )
    select
        VendorID,
        PULocationID,
        DOLocationID,
        RatecodeID,
        payment_type,
        tpep_dropoff_datetime,
        tpep_pickup_datetime,
        trip_distance,
        passenger_count,
        total_amount,
        '{month_to_load}'
    from
        '{constants.TAXI_TRIPS_TEMPLATE_FILE_PATH.format(month_to_load)}';
  """

    with database.get_connection() as conn:
        conn.execute(query)


@dg.asset(
    deps=[
        taxi_zones_file,
    ],
    group_name="ingested",
)
def taxi_zones(database: DuckDBResource) -> None:
    query = """
        create or replace table zones as (
            select
                z.LocationID as zone_id,
                z.zone as zone,
                z.borough as borough,
                z.the_geom as geometry
            from
                'data/raw/taxi_zones.csv' z
        )
    """

    with database.get_connection() as conn:
        conn.execute(query)
