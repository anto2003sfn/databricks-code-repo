# Databricks notebook source
# MAGIC %md
# MAGIC # Phase3 · SDP · 01 Bronze
# MAGIC
# MAGIC | `clinical.stream_source` | Source |
# MAGIC |---|---|
# MAGIC | `event_hubs` (default / prod) | Direct Azure Event Hubs connector |
# MAGIC | `volume` (temporary offline) | Autoloader text files under `raw_volume_path` |
# MAGIC
# MAGIC Volume mode is for pipeline bring-up **without** Event Hubs. Switch back to
# MAGIC `event_hubs` when the hub + secret + connector are ready. No Python UDF.

# COMMAND ----------
# Declarative Pipelines ignore %run, so shared config is loaded as a module.
import importlib.util
import os
import sys
from pathlib import Path


def load_sdp_config():
    candidates = []
    for key in ("clinical.repo_root", "phase3.repo_root"):
        try:
            value = spark.conf.get(key)
        except Exception:
            value = None
        if value and value.strip():
            candidates.append(value.strip())
    here = Path(os.getcwd())
    candidates += [str(here), str(here.parent), "/Workspace/Users/celin.mary@blackstraw.ai/BlackStraw/HLA/RPM/Streaming_Processing"]
    for raw in candidates:
        root = raw.rstrip("/").replace("\\", "/")
        if root.startswith(("/Repos/", "/Users/", "/Shared/")):
            root = f"/Workspace{root}"
        module_file = Path(root, "SDP", "sdp_config.py")
        if module_file.is_file():
            if root not in sys.path:
                sys.path.insert(0, root)
            spec = importlib.util.spec_from_file_location(
                "phase3_sdp_config", str(module_file)
            )
            module = importlib.util.module_from_spec(spec)
            sys.modules[spec.name] = module
            spec.loader.exec_module(module)
            return module
    raise RuntimeError(
        "SDP/sdp_config.py not found (it must exist as a workspace FILE, not a "
        "notebook). Set pipeline configuration clinical.repo_root to the folder "
        "that contains config/ and SDP/. Looked under: " + ", ".join(candidates)
    )


cfg = load_sdp_config()
stream_source = cfg.stream_source
event_type_filter = cfg.event_type_filter
raw_inbox_path = cfg.raw_inbox_path
autoloader_schema_path = cfg.autoloader_schema_path
autoloader_listing_interval = cfg.autoloader_listing_interval
event_hubs_options = cfg.event_hubs_options

# COMMAND ----------
from pyspark import pipelines as dp
from pyspark.sql import functions as F


def _string_map(column_name: str):
    empty = F.map_from_arrays(
        F.array().cast("array<string>"),
        F.array().cast("array<string>"),
    )
    return F.transform_values(
        F.coalesce(F.col(column_name), empty),
        lambda _key, value: value.cast("string"),
    )


def _bronze_from_event_hubs():
    raw = (
        spark.readStream.format("eventhubs")
        .options(**event_hubs_options())
        .load()
    )
    df = (
        raw.withColumn("properties_text", _string_map("properties"))
        .withColumn("system_properties_text", _string_map("systemProperties"))
        .select(
            F.current_timestamp().alias("ingest_ts"),
            F.to_date(F.current_timestamp()).alias("ingest_date"),
            F.lit("AZURE_EVENT_HUBS").alias("source_system"),
            F.lit("event_hubs").alias("stream_source"),
            F.col("properties_text").getItem("eventType").alias("event_type"),
            F.col("properties_text").getItem("sourceFormat").alias("source_format"),
            F.col("properties_text").getItem("deviceKey").alias("device_key"),
            F.col("properties_text").getItem("sourceTopic").alias("source_topic"),
            F.col("properties_text").getItem("messageKind").alias("message_kind"),
            F.col("properties_text").getItem("batchId").alias("batch_id"),
            F.col("properties_text").getItem("runId").alias("run_id"),
            F.col("properties_text").getItem("schemaVer").alias("schema_ver"),
            F.col("partition").cast("string").alias("eventhub_partition"),
            F.col("offset").cast("string").alias("eventhub_offset"),
            F.col("sequenceNumber").cast("long").alias("eventhub_sequence_no"),
            F.col("enqueuedTime").cast("timestamp").alias("eventhub_enqueued_ts"),
            F.col("partitionKey").cast("string").alias("partition_key"),
            F.col("body").alias("value_bytes"),
            F.col("body").cast("string").alias("value_str"),
            F.col("properties_text").alias("properties"),
            F.col("system_properties_text").alias("system_properties"),
        )
    )
    if event_type_filter:
        df = df.filter(
            (F.col("event_type") == event_type_filter)
            | F.col("event_type").isNull()
        )
    return df


def _bronze_from_volume():
    """Temporary offline ingest: Autoloader directory-listing over synthetic files."""
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "text")
        .option("cloudFiles.inferColumnTypes", "true")
        # Directory listing mode (no file-notification queue) — poll like near-realtime
        .option("cloudFiles.useNotifications", "false")
        .option("cloudFiles.useIncrementalListing", "auto")
        .option("cloudFiles.schemaLocation", autoloader_schema_path)
        .option("cloudFiles.includeExistingFiles", "true")
        .option(
            "cloudFiles.backfillInterval",
            autoloader_listing_interval,
        )
        .load(raw_inbox_path)
        .withColumnRenamed("value", "value_str")
        .withColumn("ingest_ts", F.current_timestamp())
        .withColumn("ingest_date", F.to_date(F.col("ingest_ts")))
        .withColumn("source_system", F.lit("VOLUME_LANDING"))
        .withColumn("stream_source", F.lit("volume"))
        .withColumn("path_lower", F.lower(F.col("_metadata.file_path")))
        .withColumn(
            "source_format",
            F.when(F.col("path_lower").contains("/hl7/") | F.col("path_lower").endswith(".hl7"), F.lit("HL7"))
            .when(F.col("path_lower").contains("/fhir/") | F.col("path_lower").contains(".fhir."), F.lit("FHIR"))
            .when(
                F.col("path_lower").contains("/vendor/")
                | F.col("path_lower").contains("vendor"),
                F.lit("VENDOR_JSON"),
            )
            .when(F.col("path_lower").endswith(".json"), F.lit("VENDOR_JSON"))
            .otherwise(F.lit(None)),
        )
        .withColumn(
            "device_key",
            F.when(F.col("path_lower").contains("masimo"), F.lit("MASIMO_RADIUS_VSM"))
            .when(
                F.col("path_lower").contains("portrait")
                | F.col("path_lower").contains("ge_"),
                F.lit("GE_PORTRAIT_MOBILE"),
            )
            .when(
                F.col("path_lower").contains("bx100")
                | F.col("path_lower").contains("philips"),
                F.lit("PHILIPS_BX100"),
            )
            .when(
                F.col("path_lower").contains("current_health"),
                F.lit("CURRENT_HEALTH_G2"),
            )
            .when(
                F.col("path_lower").contains("anne")
                | F.col("path_lower").contains("sibel"),
                F.lit("SIBEL_ANNE_ONE"),
            )
            .otherwise(F.lit(None)),
        )
        .withColumn(
            "message_kind",
            F.when(F.col("path_lower").contains(".oru."), F.lit("oru"))
            .when(F.col("path_lower").contains(".adt."), F.lit("adt"))
            .when(F.col("path_lower").contains("vendor"), F.lit("vendor"))
            .when(F.col("path_lower").contains("fhir"), F.lit("fhir"))
            .otherwise(F.lit(None)),
        )
        .withColumn("event_type", F.lit("CLINICAL_STANDARDS_EVENT"))
        .withColumn("source_topic", F.lit(None).cast("string"))
        .withColumn("batch_id", F.lit("volume-synth"))
        .withColumn("run_id", F.lit(None).cast("string"))
        .withColumn("schema_ver", F.lit("phase3-volume-1.0"))
        .withColumn("eventhub_partition", F.lit(None).cast("string"))
        .withColumn("eventhub_offset", F.lit(None).cast("string"))
        .withColumn("eventhub_sequence_no", F.lit(None).cast("long"))
        .withColumn("eventhub_enqueued_ts", F.lit(None).cast("timestamp"))
        .withColumn("partition_key", F.col("device_key"))
        .withColumn("value_bytes", F.col("value_str").cast("binary"))
        .withColumn(
            "properties",
            F.create_map(
                F.lit("eventType"), F.col("event_type"),
                F.lit("sourceFormat"), F.col("source_format"),
                F.lit("deviceKey"), F.col("device_key"),
                F.lit("messageKind"), F.col("message_kind"),
            ),
        )
        .withColumn(
            "system_properties",
            F.create_map(
                F.lit("ingestMode"), F.lit("volume"),
                F.lit("listingMode"), F.lit("directory"),
            ),
        )
        .drop("path_lower")
    )


@dp.table(
    name="clinical_raw_events",
    comment="Phase3 Bronze: Event Hubs (prod) or volume Autoloader (temporary offline)",
    table_properties={
        "quality": "bronze",
        "delta.enableChangeDataFeed": "true",
    },
)
@dp.expect_or_drop("has_payload", "value_str IS NOT NULL AND length(value_str) > 0")
@dp.expect("has_source_format", "source_format IS NOT NULL")
@dp.expect("has_device_key", "device_key IS NOT NULL")
def clinical_raw_events():
    if stream_source == "event_hubs":
        return _bronze_from_event_hubs()
    if stream_source == "volume":
        return _bronze_from_volume()
    raise ValueError(
        f"Unknown clinical.stream_source={stream_source!r}. "
        "Use event_hubs (prod) or volume (temporary offline demo)."
    )

