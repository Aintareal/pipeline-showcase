import inspect
import os
import sys

# spark_python_task execs this file via exec(compile(..., filename, 'exec')) rather
# than running it as a normal script, so __file__ is never defined. co_filename on the
# current frame still reflects the real path Databricks compiled, so use that instead.
_this_file = os.path.abspath(inspect.currentframe().f_code.co_filename)
sys.path.insert(0, os.path.dirname(os.path.dirname(_this_file)))

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from src.common.paths import (
    CATALOG, SCHEMA, INTERNAL_VOLUME, INBOUND_VOLUME,
    CHECKPOINT_BRONZE, SCHEMA_LOCATION_BRONZE, TBL_BRONZE,
)

spark = SparkSession.builder.getOrCreate()

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.pipeline_internal")

# NOTE: no multiLine option. Files landed in the inbound volume — whether one
# order or a batch — must be minified JSON, one complete object per line.
# multiLine=true would parse the whole file as a single value and break
# multi-object NDJSON batch files; the default line-delimited reader handles
# both a 1-line single-order file and an N-line NDJSON batch file correctly.
df = (spark.readStream
      .format("cloudFiles")
      .option("cloudFiles.format", "json")
      .option("cloudFiles.inferColumnTypes", "true")
      .option("cloudFiles.schemaLocation", SCHEMA_LOCATION_BRONZE)
      .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
      .load(INBOUND_VOLUME))

bronze = (df
          .withColumn("_source_file", col("_metadata.file_path"))
          .withColumn("_ingest_ts", current_timestamp()))

(bronze.writeStream
 .format("delta")
 .option("checkpointLocation", CHECKPOINT_BRONZE)
 .outputMode("append")
 .trigger(availableNow=True)
 .toTable(TBL_BRONZE)
 .awaitTermination())
