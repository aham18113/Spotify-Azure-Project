import dlt


# using decorators (create delta live tables (normal table, it can be a batch or streaming table) for us, depending on how we want to read the data)
@dlt.table
def dimdate_stg():
    df = spark.readStream.table("spotify_cata.silver.dimdate")
    return df


# create SCD type 2 delta live tables (auto CDC)
dlt.create_streaming_table("dimdate")
dlt.create_auto_cdc_flow(
    target="dimdate",
    source="dimdate_stg",
    keys=["date_key"],
    sequence_by="date",
    stored_as_scd_type=2,
    track_history_column_list=None,
    name=None,
    once=False,
)
