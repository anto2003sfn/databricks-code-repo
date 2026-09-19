# Databricks notebook source
# MAGIC %md
# MAGIC # Phase3 · SDP · 03 Validation views
# MAGIC
# MAGIC Declarative data-quality outputs. Pipeline expectations in 01/02 enforce
# MAGIC row-level rules; these views provide release-level failure counts.

# COMMAND ----------
from pyspark import pipelines as dp
from pyspark.sql import functions as F

SUPPORTED_RESOURCES = [
    "Patient",
    "Device",
    "Practitioner",
    "Encounter",
    "Condition",
    "MedicationRequest",
    "Observation",
    "DiagnosticReport",
]


@dp.materialized_view(
    name="fhir_validation_failures",
    comment="Structural FHIR R4 failures; this table must be empty for release",
)
def fhir_validation_failures():
    bundles = spark.read.table("fhir_bundle")
    resources = spark.read.table("fhir_resource")
    bad_bundles = bundles.filter(
        (F.get_json_object("fhir_bundle_json", "$.resourceType") != "Bundle")
        | (F.get_json_object("fhir_bundle_json", "$.type") != "collection")
        | (F.col("fhir_release") != "R4")
    ).select(
        "event_id",
        F.lit("INVALID_BUNDLE").alias("check_name"),
        F.col("fhir_bundle_json").alias("invalid_json"),
    )
    bad_resources = resources.filter(
        ~F.col("resource_type").isin(*SUPPORTED_RESOURCES)
        | F.col("resource_id").isNull()
        | (
            F.get_json_object("resource_json", "$.resourceType")
            != F.col("resource_type")
        )
        | (F.get_json_object("resource_json", "$.id") != F.col("resource_id"))
    ).select(
        "event_id",
        F.lit("INVALID_RESOURCE").alias("check_name"),
        F.col("resource_json").alias("invalid_json"),
    )
    bad_observations = resources.filter(F.col("resource_type") == "Observation").filter(
        F.get_json_object("resource_json", "$.status").isNull()
        | F.get_json_object("resource_json", "$.code").isNull()
        | F.get_json_object("resource_json", "$.subject.reference").isNull()
        | (
            F.get_json_object("resource_json", "$.valueQuantity").isNull()
            & F.get_json_object("resource_json", "$.valueCodeableConcept").isNull()
        )
    ).select(
        "event_id",
        F.lit("INVALID_OBSERVATION").alias("check_name"),
        F.col("resource_json").alias("invalid_json"),
    )
    return bad_bundles.unionByName(bad_resources).unionByName(bad_observations)


@dp.materialized_view(
    name="fhir_validation_summary",
    comment="FHIR validation failure count by check",
)
def fhir_validation_summary():
    return spark.read.table("fhir_validation_failures").groupBy("check_name").agg(
        F.count("*").alias("failed_count")
    )

