import dlt

expectations = {"rule_1": "user_id IS NOT NULL"}


# using decorators (create delta live tables (normal table, it can be a batch or streaming table) for us, depending on how we want to read the data)
@dlt.table  # decorator
@dlt.expect_all(expectations)  # applying expectations on the source table
def dimuser_stg():
    df = spark.readStream.table("spotify_cata.silver.dimuser")
    return df


# create SCD type 2 delta live tables (auto CDC)
dlt.create_streaming_table(
    name="dimuser", expect_all=expectations
)  # applying expectations on the streaming table
dlt.create_auto_cdc_flow(
    target="dimuser",
    source="dimuser_stg",
    keys=["user_id"],
    sequence_by="updated_at",
    stored_as_scd_type=2,
    track_history_column_list=None,
    name=None,
    once=False,
)
