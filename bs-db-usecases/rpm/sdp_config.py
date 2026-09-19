"""Phase3 · SDP shared config as an importable module.

Declarative Pipelines ignore `%run`, so pipeline source notebooks import this
module instead of running `00_config` as a notebook.

**Source of truth:** `config/uc_environment.yaml` (next to this repo root).

**Optional pipeline Configuration overrides** (win over YAML when set):
- `clinical.repo_root` — workspace path that contains `config/uc_environment.yaml`
- `clinical.uc_yaml_path` — full path to the YAML (optional)
- `clinical.stream_source` — `event_hubs` | `volume`
- any `clinical.*` / `phase3.*` path or Event Hubs key
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

__all__ = [
    "repo_root",
    "catalog",
    "env",
    "domain",
    "stream_source",
    "raw_volume_path",
    "raw_inbox_path",
    "autoloader_schema_path",
    "autoloader_listing_interval",
    "eh_hub",
    "eh_consumer_group",
    "eh_secret_scope",
    "eh_secret_key",
    "starting_position",
    "max_events_per_trigger",
    "event_type_filter",
    "ref_schema",
    "v_fhir_mapping_current",
    "v_fhir_resource_mapping_current",
    "event_hubs_options",
    "describe",
]


def _active_spark():
    from pyspark.sql import SparkSession

    return SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()


def _active_dbutils(spark):
    try:
        from databricks.sdk.runtime import dbutils as sdk_dbutils

        return sdk_dbutils
    except Exception:
        pass
    try:
        from pyspark.dbutils import DBUtils

        return DBUtils(spark)
    except Exception:
        return None


_spark = _active_spark()
_dbutils = _active_dbutils(_spark)


def _conf(*keys: str, default: str = "") -> str:
    for key in keys:
        try:
            val = _spark.conf.get(key)
        except Exception:
            val = None
        if val is not None and str(val).strip() != "":
            return str(val).strip()
    return default


def _workspace_path(path: str) -> str:
    p = path.strip().rstrip("/")
    if not p:
        return p
    if p.startswith("/Workspace/") or p.startswith("/Volumes/") or p.startswith("/dbfs/"):
        return p
    if p.startswith("/Repos/") or p.startswith("/Users/") or p.startswith("/Shared/"):
        return f"/Workspace{p}"
    return p


def _candidate_repo_roots() -> list[str]:
    roots: list[str] = []
    conf_root = _conf("clinical.repo_root", "phase3.repo_root", default="")
    if conf_root:
        roots.append(_workspace_path(conf_root))
    # This module lives at <repo_root>/SDP/sdp_config.py
    roots.append(str(Path(__file__).resolve().parents[1]).replace("\\", "/"))
    roots.append("/Workspace/Users/celin.mary@blackstraw.ai/BlackStraw/HLA/RPM/Streaming_Processing")
    seen: set[str] = set()
    out: list[str] = []
    for r in roots:
        r = r.rstrip("/")
        if r and r not in seen:
            seen.add(r)
            out.append(r)
    return out


def _resolve_yaml_path() -> str:
    explicit = _conf("clinical.uc_yaml_path", "phase3.uc_yaml_path", default="")
    if explicit:
        return _workspace_path(explicit)
    for root in _candidate_repo_roots():
        path = f"{root}/config/uc_environment.yaml"
        if Path(path).is_file():
            return path
    return f"{_candidate_repo_roots()[0]}/config/uc_environment.yaml"


def _load_yaml_cfg() -> dict:
    yaml_path = _resolve_yaml_path()
    repo_for_import = str(Path(yaml_path).parent.parent)
    if repo_for_import not in sys.path:
        sys.path.insert(0, repo_for_import)
    try:
        from config.uc_naming import load_uc_yaml

        cfg = load_uc_yaml(yaml_path)
        print(f"[Phase3 SDP] loaded yaml={yaml_path}")
        return cfg if isinstance(cfg, dict) else {}
    except Exception as exc:
        print(f"[Phase3 SDP] yaml not loaded ({exc}); using conf/defaults only")
        return {}


_yaml = _load_yaml_cfg()
_sdp = _yaml.get("sdp") or {}
_eh = _yaml.get("event_hubs") or {}
_ops = _yaml.get("ops") or {}

_repo_root = _conf(
    "clinical.repo_root",
    "phase3.repo_root",
    default=str(_yaml.get("repo_root") or _candidate_repo_roots()[0]),
)
repo_root = _workspace_path(_repo_root).rstrip("/")

catalog = _conf(
    "clinical.catalog",
    "phase3.catalog",
    default=str(_yaml.get("catalog") or "hla"),
)
env = _conf(
    "clinical.env",
    "phase3.env",
    default=str(_yaml.get("env") or "dev"),
).lower()
domain = (
    _conf(
        "clinical.domain",
        "phase3.domain",
        default=str(_yaml.get("domain") or "clinical"),
    )
    .lower()
    .replace("-", "_")
)

stream_source = _conf(
    "clinical.stream_source",
    "phase3.stream_source",
    default=str(_sdp.get("stream_source") or "event_hubs"),
).lower().replace("-", "_")
if stream_source in ("eventhub", "event_hub", "eh"):
    stream_source = "event_hubs"

_landing_schema = str(_ops.get("landing_schema") or "landing")
_raw_vol_name = str(
    _ops.get("landing_clinical_raw_volume")
    or _ops.get("landing_clinical_volume")
    or "clinical_raw"
)
_derived_raw_volume = (
    f"/Volumes/{catalog}/{_landing_schema}/{_raw_vol_name}/{env}"
)

raw_volume_path = _conf(
    "clinical.raw_volume_path",
    "phase3.raw_volume_path",
    default=str(_sdp.get("raw_volume_path") or "").strip() or _derived_raw_volume,
).rstrip("/")
raw_inbox_path = _conf(
    "clinical.raw_inbox_path",
    "phase3.raw_inbox_path",
    default=str(_sdp.get("raw_inbox_path") or "").strip() or f"{raw_volume_path}/inbox",
).rstrip("/")
autoloader_schema_path = _conf(
    "clinical.autoloader_schema_path",
    "phase3.autoloader_schema_path",
    default=str(_sdp.get("autoloader_schema_path") or "").strip()
    or f"{raw_volume_path}/_autoloader_schema",
).rstrip("/")
autoloader_listing_interval = _conf(
    "clinical.autoloader_listing_interval",
    "phase3.autoloader_listing_interval",
    default=str(_sdp.get("autoloader_listing_interval") or "30 seconds"),
)

eh_hub = _conf(
    "clinical.eh_hub",
    "phase3.eh_hub",
    default=str(_eh.get("hub") or "hla-events"),
)
eh_consumer_group = _conf(
    "clinical.eh_consumer_group",
    "phase3.eh_consumer_group",
    default=str(_eh.get("consumer_group_sdp") or "clinical-fhir-sdp-dev"),
)
eh_secret_scope = _conf(
    "clinical.eh_secret_scope",
    "phase3.eh_secret_scope",
    default=str(_eh.get("secret_scope") or "hla"),
)
eh_secret_key = _conf(
    "clinical.eh_secret_key",
    "phase3.eh_secret_key",
    default=str(_eh.get("secret_key") or "eventhub-listen-conn-str"),
)
starting_position = _conf(
    "clinical.starting_position",
    "phase3.starting_position",
    default=str(_eh.get("starting_position") or "earliest"),
).lower()
max_events_per_trigger = _conf(
    "clinical.max_events_per_trigger",
    "phase3.max_events_per_trigger",
    default=str(_eh.get("max_events_per_trigger") or "50000"),
)
event_type_filter = _conf(
    "clinical.event_type_filter",
    "phase3.event_type_filter",
    default=str(_eh.get("event_type_filter") or "CLINICAL_STANDARDS_EVENT"),
)

ref_schema = f"{catalog}.{domain}_ref_{env}"
v_fhir_mapping_current = f"{ref_schema}.v_fhir_mapping_current"
v_fhir_resource_mapping_current = f"{ref_schema}.v_fhir_resource_mapping_current"


def _with_entity_path(connection_string: str) -> str:
    if "EntityPath=" in connection_string:
        return connection_string
    return f"{connection_string.rstrip(';')};EntityPath={eh_hub}"


def event_hubs_options() -> dict[str, str]:
    if _dbutils is None:
        raise RuntimeError(
            "dbutils is unavailable; cannot read the Event Hubs listen secret "
            f"({eh_secret_scope}/{eh_secret_key})."
        )
    connection_string = _with_entity_path(
        _dbutils.secrets.get(scope=eh_secret_scope, key=eh_secret_key)
    )
    encrypted = _spark.sparkContext._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(
        connection_string
    )
    position = {
        "offset": "-1" if starting_position == "earliest" else "@latest",
        "seqNo": -1,
        "enqueuedTime": None,
        "isInclusive": True,
    }
    return {
        "eventhubs.connectionString": encrypted,
        "eventhubs.consumerGroup": eh_consumer_group,
        "eventhubs.startingPosition": json.dumps(position),
        "eventhubs.maxEventsPerTrigger": max_events_per_trigger,
    }


def describe() -> None:
    print(
        f"[Phase3 SDP] catalog={catalog} domain={domain} env={env} "
        f"stream_source={stream_source} ref={ref_schema} repo_root={repo_root}"
    )
    if stream_source == "volume":
        print(
            f"[Phase3 SDP] inbox={raw_inbox_path} schema={autoloader_schema_path} "
            f"listing_interval={autoloader_listing_interval}"
        )
    else:
        print(f"[Phase3 SDP] hub={eh_hub} cg={eh_consumer_group}")


describe()
