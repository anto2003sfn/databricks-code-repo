# Databricks notebook source
# MAGIC %md
# MAGIC # Phase3 · SDP · 02 Normalize to FHIR R4
# MAGIC
# MAGIC Declarative Bronze → context/measurement → FHIR resource → Bundle graph.
# MAGIC All parsing and JSON construction uses Spark SQL functions; no Python UDF.

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
v_fhir_mapping_current = cfg.v_fhir_mapping_current

# COMMAND ----------
from functools import reduce

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql import types as T

FHIR_TAG_SYSTEM = "https://example.org/hla/sensitivity"
LOINC_SYSTEM = "http://loinc.org"
OBS_CATEGORY_SYSTEM = "http://terminology.hl7.org/CodeSystem/observation-category"

VENDOR_OBSERVATION_SCHEMA = T.ArrayType(
    T.StructType(
        [
            T.StructField("metric", T.StringType()),
            T.StructField("loinc", T.StringType()),
            T.StructField("unit", T.StringType()),
            T.StructField("value", T.DoubleType()),
            T.StructField("valueText", T.StringType()),
            T.StructField("recordedAt", T.StringType()),
        ]
    )
)


def safe_id(column):
    return F.lower(
        F.regexp_replace(F.trim(column.cast("string")), r"[^A-Za-z0-9\-.]", "-")
    )


def first_component(column):
    return F.element_at(F.split(column, r"\^"), 1)


def hl7_field(segment, field_number: int):
    return F.element_at(F.split(segment, r"\|"), field_number + 1)


def nonempty(column):
    return F.when(F.length(F.trim(column)) > 0, F.trim(column))


def normalize_source_format(column):
    upper = F.upper(column)
    return (
        F.when(upper.isin("HL7", "HL7_V2"), "HL7_V2")
        .when(upper.isin("VENDOR", "VENDOR_JSON", "JSON"), "VENDOR_JSON")
        .when(upper.isin("FHIR", "FHIR_R4"), "FHIR_R4")
        .otherwise(upper)
    )


def with_identity(df):
    fallback = F.sha2(
        F.concat_ws(
            "||",
            F.coalesce("device_key", F.lit("")),
            F.coalesce("value_str", F.lit("")),
            F.coalesce(F.col("ingest_ts").cast("string"), F.lit("")),
        ),
        256,
    )
    event_id = F.coalesce(
        F.when(
            F.col("eventhub_partition").isNotNull()
            & F.col("eventhub_offset").isNotNull(),
            F.concat_ws(
                "-", "eventhub_partition", "eventhub_offset"
            ),
        ),
        fallback,
    )
    return (
        df.withColumn("source_format", normalize_source_format("source_format"))
        .withColumn("event_id", event_id)
        .withColumn(
            "bundle_id", F.concat(F.lit("bundle-"), F.substring(event_id, 1, 32))
        )
    )


def parse_context(df):
    base = (
        with_identity(df)
        .withColumn(
            "_segments",
            F.split(F.regexp_replace("value_str", r"\n", "\r"), r"\r+"),
        )
        .withColumn(
            "_pid",
            F.element_at(F.filter("_segments", lambda x: x.startswith("PID|")), 1),
        )
        .withColumn(
            "_pv1",
            F.element_at(F.filter("_segments", lambda x: x.startswith("PV1|")), 1),
        )
        .withColumn(
            "_obr",
            F.element_at(F.filter("_segments", lambda x: x.startswith("OBR|")), 1),
        )
    )
    pid_name = hl7_field(F.col("_pid"), 5)
    attending = hl7_field(F.col("_pv1"), 7)
    hl7_gender = F.upper(nonempty(hl7_field(F.col("_pid"), 8)))
    hl7_class = F.upper(nonempty(hl7_field(F.col("_pv1"), 2)))
    return (
        base.withColumn(
            "patient_id",
            F.when(
                F.col("source_format") == "HL7_V2",
                first_component(hl7_field(F.col("_pid"), 3)),
            ).when(
                F.col("source_format") == "VENDOR_JSON",
                F.get_json_object("value_str", "$.patient.patientId"),
            ),
        )
        .withColumn(
            "mrn",
            F.when(
                F.col("source_format") == "HL7_V2",
                first_component(hl7_field(F.col("_pid"), 3)),
            ).otherwise(F.get_json_object("value_str", "$.patient.mrn")),
        )
        .withColumn(
            "patient_family",
            F.when(
                F.col("source_format") == "HL7_V2",
                F.element_at(F.split(pid_name, r"\^"), 1),
            ),
        )
        .withColumn(
            "patient_given",
            F.when(
                F.col("source_format") == "HL7_V2",
                F.element_at(F.split(pid_name, r"\^"), 2),
            ),
        )
        .withColumn(
            "patient_name_text",
            F.when(
                F.col("source_format") == "VENDOR_JSON",
                F.get_json_object("value_str", "$.patient.name"),
            ),
        )
        .withColumn(
            "birth_date",
            F.when(
                (F.col("source_format") == "HL7_V2")
                & (F.length(hl7_field(F.col("_pid"), 7)) == 8),
                F.concat_ws(
                    "-",
                    F.substring(hl7_field(F.col("_pid"), 7), 1, 4),
                    F.substring(hl7_field(F.col("_pid"), 7), 5, 2),
                    F.substring(hl7_field(F.col("_pid"), 7), 7, 2),
                ),
            ),
        )
        .withColumn(
            "gender",
            F.when(hl7_gender == "M", "male")
            .when(hl7_gender == "F", "female")
            .when(hl7_gender == "O", "other")
            .when(hl7_gender.isNotNull(), "unknown"),
        )
        .withColumn(
            "encounter_id",
            F.when(
                F.col("source_format") == "HL7_V2",
                first_component(hl7_field(F.col("_pv1"), 19)),
            ).otherwise(F.get_json_object("value_str", "$.patient.encounterId")),
        )
        .withColumn(
            "encounter_class",
            F.when(hl7_class == "I", "IMP")
            .when(hl7_class == "E", "EMER")
            .otherwise("AMB"),
        )
        .withColumn(
            "practitioner_id",
            F.when(
                F.col("source_format") == "HL7_V2", first_component(attending)
            ).otherwise(
                F.get_json_object("value_str", "$.careTeam.practitionerId")
            ),
        )
        .withColumn(
            "practitioner_name",
            F.when(
                F.col("source_format") == "HL7_V2",
                F.concat_ws(
                    " ",
                    F.element_at(F.split(attending, r"\^"), 3),
                    F.element_at(F.split(attending, r"\^"), 2),
                ),
            ).otherwise(
                F.get_json_object("value_str", "$.careTeam.practitionerName")
            ),
        )
        .withColumn(
            "condition_text",
            F.when(
                F.col("source_format") == "VENDOR_JSON",
                F.get_json_object("value_str", "$.clinical.primaryCondition"),
            ),
        )
        .withColumn(
            "medication_text",
            F.when(
                F.col("source_format") == "VENDOR_JSON",
                F.get_json_object("value_str", "$.clinical.medication"),
            ),
        )
        .withColumn(
            "manufacturer",
            F.when(
                F.col("source_format") == "VENDOR_JSON",
                F.get_json_object("value_str", "$.vendor"),
            ),
        )
        .withColumn(
            "device_model",
            F.when(
                F.col("source_format") == "VENDOR_JSON",
                F.get_json_object("value_str", "$.model"),
            ),
        )
        .withColumn(
            "effective_ts",
            F.coalesce(
                F.to_timestamp(
                    F.when(
                        F.col("source_format") == "HL7_V2",
                        hl7_field(F.col("_obr"), 7),
                    ),
                    "yyyyMMddHHmmss",
                ),
                F.col("eventhub_enqueued_ts"),
                F.col("ingest_ts"),
            ),
        )
        .withColumn("patient_id", safe_id(nonempty("patient_id")))
        .withColumn("encounter_id", safe_id(nonempty("encounter_id")))
        .withColumn("practitioner_id", safe_id(nonempty("practitioner_id")))
        .withColumn("device_id", safe_id("device_key"))
    )


@dp.table(
    name="normalized_event_context",
    comment="Typed identity and clinical context parsed without a UDF",
    table_properties={"quality": "silver-internal"},
)
def normalized_event_context():
    return parse_context(spark.readStream.table("clinical_raw_events"))


def measurement_identity():
    return [
        "event_id",
        "ingest_ts",
        "ingest_date",
        "source_format",
        "device_key",
        "eventhub_partition",
        "eventhub_offset",
        "bundle_id",
        "patient_id",
        "encounter_id",
        "device_id",
        "effective_ts",
        "value_str",
    ]


@dp.table(
    name="source_measurement",
    comment="One source measurement per HL7 OBX or vendor observations item",
    table_properties={"quality": "silver-internal"},
)
def source_measurement():
    context = spark.readStream.table("normalized_event_context")
    vendor = (
        context.filter(F.col("source_format") == "VENDOR_JSON")
        .withColumn(
            "_measurement",
            F.explode_outer(
                F.from_json(
                    F.get_json_object("value_str", "$.observations"),
                    VENDOR_OBSERVATION_SCHEMA,
                )
            ),
        )
        .select(
            *measurement_identity(),
            F.col("_measurement.metric").alias("source_field"),
            F.coalesce("_measurement.loinc", "_measurement.metric").alias(
                "source_code"
            ),
            F.col("_measurement.unit").alias("source_unit"),
            F.col("_measurement.value").cast("string").alias("source_value"),
            F.col("_measurement.valueText").alias("source_value_text"),
            F.coalesce(
                F.to_timestamp("_measurement.recordedAt"), "effective_ts"
            ).alias("measurement_ts"),
        )
        .filter(F.col("source_field").isNotNull())
    )
    hl7 = (
        context.filter(F.col("source_format") == "HL7_V2")
        .withColumn(
            "_obx", F.explode_outer(F.filter("_segments", lambda x: x.startswith("OBX|")))
        )
        .select(
            *measurement_identity(),
            F.element_at(F.split(hl7_field(F.col("_obx"), 3), r"\^"), 2).alias(
                "source_field"
            ),
            first_component(hl7_field(F.col("_obx"), 3)).alias("source_code"),
            first_component(hl7_field(F.col("_obx"), 6)).alias("source_unit"),
            hl7_field(F.col("_obx"), 5).alias("source_value"),
            F.lit(None).cast("string").alias("source_value_text"),
            F.coalesce(
                F.to_timestamp(hl7_field(F.col("_obx"), 14), "yyyyMMddHHmmss"),
                "effective_ts",
            ).alias("measurement_ts"),
        )
        .filter(F.col("source_code").isNotNull() & (F.col("source_code") != "DEVICE_KEY"))
    )
    return vendor.unionByName(hl7)


def matched_measurements():
    source = spark.readStream.table("source_measurement").alias("s")
    mapping = (
        spark.read.table(v_fhir_mapping_current)
        .filter(F.col("fhir_resource_type") == "Observation")
        .alias("m")
    )
    condition = (
        (F.col("s.device_key") == F.col("m.device_key"))
        & (F.col("s.source_format") == F.col("m.source_format"))
        & (
            ((F.col("s.source_format") == "HL7_V2") & (F.col("s.source_code") == F.col("m.source_code")))
            | ((F.col("s.source_format") == "VENDOR_JSON") & ((F.col("s.source_field") == F.col("m.source_field")) | (F.col("s.source_code") == F.col("m.source_code"))))
        )
    )
    return source.join(F.broadcast(mapping), condition, "left")


@dp.table(
    name="fhir_observation",
    comment="Normalized FHIR R4 Observation JSON built from approved mapping metadata",
    table_properties={"quality": "silver"},
)
@dp.expect_or_drop("mapped", "mapping_id IS NOT NULL")
@dp.expect_or_drop(
    "valid_value",
    "(fhir_value_type = 'quantity' AND value_num IS NOT NULL) OR fhir_value_type = 'codeable'",
)
def fhir_observation():
    matched = matched_measurements().filter(F.col("m.mapping_id").isNotNull())
    observation_id = F.concat(
        F.lit("obs-"),
        F.substring(
            F.sha2(
                F.concat_ws("||", "s.event_id", "m.mapping_id", F.col("s.measurement_ts").cast("string")),
                256,
            ),
            1,
            24,
        ),
    )
    value_num = F.when(
        F.col("m.fhir_value_type") == "quantity",
        F.col("s.source_value").cast("double"),
    )
    value_text = F.coalesce("s.source_value_text", "s.source_value")
    resource_json = F.to_json(
        F.struct(
            F.lit("Observation").alias("resourceType"),
            observation_id.alias("id"),
            F.struct(
                F.array(
                    F.struct(F.lit(FHIR_TAG_SYSTEM).alias("system"), F.col("m.sensitivity_class").alias("code")),
                    F.struct(F.lit("https://example.org/hla/measurementClass").alias("system"), F.col("m.measurement_class").alias("code")),
                ).alias("tag")
            ).alias("meta"),
            F.col("m.fhir_status").alias("status"),
            F.array(F.struct(F.array(F.struct(F.lit(OBS_CATEGORY_SYSTEM).alias("system"), F.col("m.observation_category_code").alias("code"), F.lit("Vital Signs").alias("display"))).alias("coding"))).alias("category"),
            F.struct(F.array(F.struct(F.col("m.target_code_system").alias("system"), F.col("m.target_code").alias("code"), F.col("m.target_display").alias("display"))).alias("coding"), F.col("m.target_display").alias("text")).alias("code"),
            F.struct(F.concat(F.lit("Patient/"), F.col("s.patient_id")).alias("reference")).alias("subject"),
            F.when(F.col("s.encounter_id").isNotNull(), F.struct(F.concat(F.lit("Encounter/"), F.col("s.encounter_id")).alias("reference"))).alias("encounter"),
            F.date_format(F.col("s.measurement_ts"), "yyyy-MM-dd'T'HH:mm:ssXXX").alias("effectiveDateTime"),
            F.struct(F.concat(F.lit("Device/"), F.col("s.device_id")).alias("reference")).alias("device"),
            F.when(F.col("m.fhir_value_type") == "quantity", F.struct(value_num.alias("value"), F.col("m.target_unit_display").alias("unit"), F.col("m.target_unit_system").alias("system"), F.col("m.target_unit_code").alias("code"))).alias("valueQuantity"),
            F.when(F.col("m.fhir_value_type") == "codeable", F.struct(value_text.alias("text"))).alias("valueCodeableConcept"),
        ),
        options={"ignoreNullFields": "true"},
    )
    return matched.select(
        F.col("s.event_id").alias("event_id"),
        F.col("s.ingest_ts").alias("ingest_ts"),
        F.col("s.ingest_date").alias("ingest_date"),
        F.col("s.source_format").alias("source_format"),
        F.col("s.device_key").alias("device_key"),
        F.col("s.eventhub_partition").alias("eventhub_partition"),
        F.col("s.eventhub_offset").alias("eventhub_offset"),
        F.col("m.mapping_version").alias("mapping_version"),
        F.col("s.bundle_id").alias("bundle_id"),
        F.lit("Observation").alias("resource_type"),
        observation_id.alias("resource_id"),
        resource_json.alias("resource_json"),
        value_num.alias("value_num"),
        F.col("m.fhir_value_type").alias("fhir_value_type"),
        F.current_timestamp().alias("normalize_ts"),
        F.col("m.mapping_id").alias("mapping_id"),
    )


def resource_frame(context, resource_type, id_col, json_col, predicate):
    version = spark.read.table(v_fhir_mapping_current).agg(
        F.max("mapping_version").alias("mapping_version")
    )
    return (
        context.filter(predicate)
        .crossJoin(F.broadcast(version))
        .select(
            "event_id", "ingest_ts", "ingest_date", "source_format", "device_key",
            "eventhub_partition", "eventhub_offset", "mapping_version", "bundle_id",
            F.lit(resource_type).alias("resource_type"), id_col.alias("resource_id"),
            json_col.alias("resource_json"), F.current_timestamp().alias("normalize_ts"),
        )
    )


@dp.table(
    name="fhir_context_resource",
    comment="Patient, Device, Practitioner, Encounter, Condition and MedicationRequest JSON",
    table_properties={"quality": "silver"},
)
def fhir_context_resource():
    context = spark.readStream.table("normalized_event_context").filter(
        F.col("patient_id").isNotNull() & F.col("device_id").isNotNull()
    )
    patient_json = F.to_json(F.struct(F.lit("Patient").alias("resourceType"), F.col("patient_id").alias("id"), F.struct(F.array(F.struct(F.lit(FHIR_TAG_SYSTEM).alias("system"), F.lit("PHI").alias("code"))).alias("tag")).alias("meta"), F.when(F.col("mrn").isNotNull(), F.array(F.struct(F.lit("https://example.org/mrn").alias("system"), F.col("mrn").alias("value")))).alias("identifier"), F.when(F.col("patient_name_text").isNotNull() | F.col("patient_family").isNotNull(), F.array(F.struct(F.col("patient_name_text").alias("text"), F.col("patient_family").alias("family"), F.when(F.col("patient_given").isNotNull(), F.array("patient_given")).alias("given")))).alias("name"), F.col("gender").alias("gender"), F.col("birth_date").alias("birthDate")), options={"ignoreNullFields": "true"})
    device_json = F.to_json(F.struct(F.lit("Device").alias("resourceType"), F.col("device_id").alias("id"), F.struct(F.array(F.struct(F.lit(FHIR_TAG_SYSTEM).alias("system"), F.lit("SENSITIVE").alias("code"))).alias("tag")).alias("meta"), F.array(F.struct(F.lit("https://example.org/hla/deviceKey").alias("system"), F.col("device_key").alias("value"))).alias("identifier"), F.col("manufacturer").alias("manufacturer"), F.when(F.col("device_model").isNotNull(), F.array(F.struct(F.lit("model-name").alias("type"), F.col("device_model").alias("name")))).alias("deviceName")), options={"ignoreNullFields": "true"})
    practitioner_json = F.to_json(F.struct(F.lit("Practitioner").alias("resourceType"), F.col("practitioner_id").alias("id"), F.when(F.col("practitioner_name").isNotNull(), F.array(F.struct(F.col("practitioner_name").alias("text")))).alias("name")), options={"ignoreNullFields": "true"})
    encounter_json = F.to_json(F.struct(F.lit("Encounter").alias("resourceType"), F.col("encounter_id").alias("id"), F.lit("in-progress").alias("status"), F.struct(F.lit("http://terminology.hl7.org/CodeSystem/v3-ActCode").alias("system"), F.col("encounter_class").alias("code")).alias("class"), F.struct(F.concat(F.lit("Patient/"), "patient_id").alias("reference")).alias("subject")), options={"ignoreNullFields": "true"})
    condition_id = F.concat(F.lit("condition-"), F.substring(F.sha2(F.concat_ws("||", "event_id", "condition_text"), 256), 1, 24))
    condition_json = F.to_json(F.struct(F.lit("Condition").alias("resourceType"), condition_id.alias("id"), F.struct(F.col("condition_text").alias("text")).alias("code"), F.struct(F.concat(F.lit("Patient/"), "patient_id").alias("reference")).alias("subject")), options={"ignoreNullFields": "true"})
    med_id = F.concat(F.lit("medreq-"), F.substring(F.sha2(F.concat_ws("||", "event_id", "medication_text"), 256), 1, 24))
    med_json = F.to_json(F.struct(F.lit("MedicationRequest").alias("resourceType"), med_id.alias("id"), F.lit("active").alias("status"), F.lit("order").alias("intent"), F.struct(F.col("medication_text").alias("text")).alias("medicationCodeableConcept"), F.struct(F.concat(F.lit("Patient/"), "patient_id").alias("reference")).alias("subject")), options={"ignoreNullFields": "true"})
    frames = [
        resource_frame(context, "Patient", F.col("patient_id"), patient_json, F.col("patient_id").isNotNull()),
        resource_frame(context, "Device", F.col("device_id"), device_json, F.col("device_id").isNotNull()),
        resource_frame(context, "Practitioner", F.col("practitioner_id"), practitioner_json, F.col("practitioner_id").isNotNull()),
        resource_frame(context, "Encounter", F.col("encounter_id"), encounter_json, F.col("encounter_id").isNotNull()),
        resource_frame(context, "Condition", condition_id, condition_json, F.col("condition_text").isNotNull()),
        resource_frame(context, "MedicationRequest", med_id, med_json, F.col("medication_text").isNotNull()),
    ]
    return reduce(lambda left, right: left.unionByName(right), frames)


@dp.table(name="fhir_resource_generated", comment="Streaming generated FHIR resources")
def fhir_resource_generated():
    context = spark.readStream.table("fhir_context_resource")
    observation = spark.readStream.table("fhir_observation").select(context.columns)
    return context.unionByName(observation)


@dp.materialized_view(name="diagnostic_report", comment="Derived vital-signs DiagnosticReport")
def diagnostic_report():
    obs = spark.read.table("fhir_observation")
    grouped = obs.groupBy("event_id", "ingest_ts", "ingest_date", "source_format", "device_key", "eventhub_partition", "eventhub_offset", "mapping_version", "bundle_id").agg(F.collect_list("resource_id").alias("observation_ids"))
    report_id = F.concat(F.lit("diagnosticreport-"), F.substring(F.sha2("event_id", 256), 1, 24))
    report_json = F.to_json(F.struct(F.lit("DiagnosticReport").alias("resourceType"), report_id.alias("id"), F.lit("final").alias("status"), F.struct(F.array(F.struct(F.lit(LOINC_SYSTEM).alias("system"), F.lit("85353-1").alias("code"), F.lit("Vital signs panel").alias("display"))).alias("coding")).alias("code"), F.transform("observation_ids", lambda x: F.struct(F.concat(F.lit("Observation/"), x).alias("reference"))).alias("result")), options={"ignoreNullFields": "true"})
    return grouped.select("event_id", "ingest_ts", "ingest_date", "source_format", "device_key", "eventhub_partition", "eventhub_offset", "mapping_version", "bundle_id", F.lit("DiagnosticReport").alias("resource_type"), report_id.alias("resource_id"), report_json.alias("resource_json"), F.current_timestamp().alias("normalize_ts"))


@dp.materialized_view(name="fhir_resource", comment="All generated FHIR R4 resources")
def fhir_resource():
    return spark.read.table("fhir_resource_generated").unionByName(spark.read.table("diagnostic_report"))


@dp.materialized_view(name="fhir_bundle_generated", comment="FHIR R4 collection Bundles generated from HL7/vendor inputs")
def fhir_bundle_generated():
    resources = spark.read.table("fhir_resource")
    entry = F.concat(F.lit('{"fullUrl":"urn:uuid:'), "resource_id", F.lit('","resource":'), "resource_json", F.lit("}"))
    return (
        resources.withColumn("_entry", entry)
        .groupBy("event_id", "ingest_ts", "ingest_date", "source_format", "device_key", "eventhub_partition", "eventhub_offset", "mapping_version", "bundle_id")
        .agg(F.sort_array(F.collect_list("_entry")).alias("entries"), F.count("*").cast("int").alias("resource_count"), F.max("normalize_ts").alias("normalize_ts"))
        .withColumn("fhir_bundle_json", F.concat(F.lit('{"resourceType":"Bundle","id":"'), "bundle_id", F.lit('","type":"collection","entry":['), F.concat_ws(",", "entries"), F.lit("]}")))
        .select("event_id", "ingest_ts", "ingest_date", "source_format", "device_key", "eventhub_partition", "eventhub_offset", "mapping_version", "bundle_id", F.lit("R4").alias("fhir_release"), "resource_count", "fhir_bundle_json", "normalize_ts")
    )


@dp.table(name="fhir_bundle_native", comment="Validated native FHIR R4 Bundle pass-through")
@dp.expect_or_drop("is_bundle", "get_json_object(fhir_bundle_json, '$.resourceType') = 'Bundle'")
def fhir_bundle_native():
    src = with_identity(spark.readStream.table("clinical_raw_events")).filter(F.col("source_format") == "FHIR_R4")
    entry_schema = T.ArrayType(T.StructType([T.StructField("fullUrl", T.StringType())]))
    return src.filter(F.get_json_object("value_str", "$.entry").isNotNull()).select("event_id", "ingest_ts", "ingest_date", "source_format", "device_key", "eventhub_partition", "eventhub_offset", F.lit("native-r4").alias("mapping_version"), F.coalesce(F.get_json_object("value_str", "$.id"), "bundle_id").alias("bundle_id"), F.lit("R4").alias("fhir_release"), F.size(F.from_json(F.get_json_object("value_str", "$.entry"), entry_schema)).cast("int").alias("resource_count"), F.col("value_str").alias("fhir_bundle_json"), F.current_timestamp().alias("normalize_ts"))


@dp.materialized_view(name="fhir_bundle", comment="Final generated and native normalized FHIR R4 Bundles")
def fhir_bundle():
    return spark.read.table("fhir_bundle_generated").unionByName(spark.read.table("fhir_bundle_native"))


@dp.table(name="fhir_normalization_quarantine", comment="Unsupported, invalid or unmapped source data")
def fhir_normalization_quarantine():
    bronze = with_identity(spark.readStream.table("clinical_raw_events"))
    unsupported = bronze.filter(~F.col("source_format").isin("HL7_V2", "VENDOR_JSON", "FHIR_R4") | F.col("source_format").isNull()).select("event_id", "ingest_ts", "ingest_date", "source_format", "device_key", "eventhub_partition", "eventhub_offset", F.lit("UNSUPPORTED_SOURCE_FORMAT").alias("error_code"), F.lit("sourceFormat must be HL7, VENDOR_JSON, or FHIR").alias("error_message"), F.col("value_str").alias("source_value"), F.current_timestamp().alias("quarantine_ts"))
    context = spark.readStream.table("normalized_event_context")
    missing = context.filter(F.col("source_format").isin("HL7_V2", "VENDOR_JSON") & (F.col("patient_id").isNull() | F.col("device_id").isNull())).select("event_id", "ingest_ts", "ingest_date", "source_format", "device_key", "eventhub_partition", "eventhub_offset", F.lit("MISSING_REQUIRED_CONTEXT").alias("error_code"), F.lit("Patient identifier and device_key are required").alias("error_message"), F.col("value_str").alias("source_value"), F.current_timestamp().alias("quarantine_ts"))
    unmatched = matched_measurements().filter(F.col("m.mapping_id").isNull()).select(F.col("s.event_id").alias("event_id"), F.col("s.ingest_ts").alias("ingest_ts"), F.col("s.ingest_date").alias("ingest_date"), F.col("s.source_format").alias("source_format"), F.col("s.device_key").alias("device_key"), F.col("s.eventhub_partition").alias("eventhub_partition"), F.col("s.eventhub_offset").alias("eventhub_offset"), F.lit("UNMAPPED_MEASUREMENT").alias("error_code"), F.concat_ws(" ", F.lit("No approved mapping for"), F.col("s.source_code")).alias("error_message"), F.to_json(F.struct(F.col("s.source_field"), F.col("s.source_code"), F.col("s.source_unit"), F.col("s.source_value"))).alias("source_value"), F.current_timestamp().alias("quarantine_ts"))
    return unsupported.unionByName(missing).unionByName(unmatched)

