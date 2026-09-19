# Phase3 Spark Declarative Pipelines (SDP/DLT)

Declarative Bronze → Spark-native FHIR R4 normalize → Bundles / quarantine.
No Python UDF.

| Mode | Ingest | When |
|---|---|---|
| **volume** (temporary) | Autoloader directory listing on UC Volume inbox | No Event Hubs yet |
| **event_hubs** (prod) | Direct Azure Event Hubs connector | Hub + secret + connector ready |

---

## Mock realtime (volume) — recommended first

```text
Local synthetic files  --paced copy (30s)-->  UC Volume inbox/
        Autoloader directory listing (~30s) --> SDP Bronze --> normalize --> Silver
```

### Unity Catalog objects

| Object | Name / path |
|---|---|
| Schema | `hla.landing` |
| Volume | `hla.landing.clinical_raw` |
| Inbox (Autoloader root) | `/Volumes/hla/landing/clinical_raw/dev/inbox/{hl7,vendor,fhir}` |
| Autoloader schema | `/Volumes/hla/landing/clinical_raw/dev/_autoloader_schema` |
| SDP target | catalog `hla`, schema `clinical_sdp_dev` |
| Mapping ref | `hla.clinical_ref_dev` |

Create via `notebooks/00_uc_bootstrap.py` (`dry_run=false`) or the land notebook.

### No magic commands in pipeline source

Declarative Pipelines ignore magic commands (`%run`, `%sql`, …) except `%pip`.
`01` and `02` therefore load `SDP/sdp_config.py` as a Python module. The loader
looks for that file under `clinical.repo_root`, then the notebook working
directory and its parent — so **`config/` and `SDP/` must sit under the same
folder you set as `clinical.repo_root`**.

### Config (YAML first)

| Source | Used by | Notes |
|---|---|---|
| `config/uc_environment.yaml` | `SDP/00_config.py` | catalog, env, domain, `sdp.*`, Event Hubs |
| Pipeline Configuration | overrides YAML | only `clinical.repo_root` required |
| `ehr_batch/00_config.py` widgets | EHR batch only | **unchanged** by this SDP change |

### Setup steps

1. **Sync** `Phase3/` (must include `config/uc_environment.yaml`) → workspace
2. **Bootstrap UC** — `notebooks/00_uc_bootstrap.py`
3. **Mappings** — DDL + notebooks `01` → `02` → `03` (must pass)
4. **Create pipeline** from `SDP/pipeline_settings_volume.json`
   - Configuration: `clinical.repo_root` = folder that contains `config/`
   - Optional: `pipelines.trigger.interval=30 seconds`
   - Defaults (`sdp.stream_source=volume`, paths) come from YAML
   - **No** Event Hubs connector / secret for volume mode
5. **Start** the pipeline (Triggered is OK if Continuous is blocked in UI)
6. **Feed files** (pick one):

**A. From your laptop (preferred paced mock)**

```bash
# Auth once: databricks auth login
cd Phase3
py -3 scripts/stream_synthetic_to_landing.py --interval-seconds 30

# optional: regenerate Phase2 samples first
# cd ../Phase2 && py -3 generate_clinical_standards.py generate --out sample_data
# py -3 scripts/stream_synthetic_to_landing.py --source ../Phase2/sample_data --interval-seconds 30 --loop
```

**B. From Databricks (samples already in workspace)**

Run `SDP/land_synthetic_clinical_raw.py` with widgets:

| Widget | Value |
|---|---|
| `mode` | `paced` |
| `interval_seconds` | `30` |
| `clear_inbox` | `true` on first run (optional) |

7. **Validate**

```sql
SELECT source_format, device_key, COUNT(*)
FROM hla.clinical_sdp_dev.clinical_raw_events
GROUP BY 1, 2;

SELECT COUNT(*) FROM hla.clinical_sdp_dev.fhir_bundle;
SELECT COUNT(*) FROM hla.clinical_sdp_dev.fhir_normalization_quarantine;
SELECT * FROM hla.clinical_sdp_dev.fhir_validation_failures;
```

### Files involved

| File | Role |
|---|---|
| `SDP/sdp_config.py` | Shared config **module** (pipelines import it; `%run` is unsupported) |
| `SDP/00_config.py` | Interactive-only shim that loads `sdp_config.py` |
| `scripts/stream_synthetic_to_landing.py` | Local → Volume, one file / N seconds |
| `SDP/land_synthetic_clinical_raw.py` | Workspace paced/bulk land |
| `SDP/pipeline_settings_volume.json` | Continuous volume pipeline conf |
| `sample_test_data/landing/clinical_raw/` | Built-in HL7 / vendor / FHIR samples |

---

## Prod path — Event Hubs

Use `pipeline_settings.json`, attach
`com.microsoft.azure:azure-eventhubs-spark_2.12:<certified>`, set Listen secret,
`clinical.stream_source=event_hubs`.

---

## SDP vs Structured Streaming

| Area | `spark_ss` | `SDP` |
|---|---|---|
| Orchestration | Job task chain | Managed pipeline graph |
| Offline mock | Not wired (use SDP volume) | Autoloader volume mode |
| Prod ingest | Event Hubs connector | Event Hubs connector |
| Quality | Quarantine + validate notebook | Expectations + validation views |
