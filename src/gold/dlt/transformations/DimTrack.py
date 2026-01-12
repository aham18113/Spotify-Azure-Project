import dlt


# using decorators (create delta live tables (normal table, it can be a batch or streaming table) for us, depending on how we want to read the data)
@dlt.table
def dimtrack_stg():
    df = spark.readStream.table("spotify_cata.silver.dimtrack")
    return df


# create SCD type 2 delta live tables (auto CDC)
dlt.create_streaming_table("dimtrack")
dlt.create_auto_cdc_flow(
    target="dimtrack",
    source="dimtrack_stg",
    keys=["track_id"],
    sequence_by="updated_at",
    stored_as_scd_type=2,
    track_history_column_list=None,
    name=None,
    once=False,
)
