from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp
from src.common.paths import INBOUND_VOLUME


@dp.table(name="bronze_orders", comment="Raw landed orders via Auto Loader.")
def bronze_orders():
    return (spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .load(INBOUND_VOLUME)
            .withColumn("_source_file", col("_metadata.file_path"))
            .withColumn("_ingest_ts", current_timestamp()))
