"""Unity Catalog naming helpers (local + Databricks).

Industry pattern:
  {catalog}.{domain}_{layer}_{env}

domain = business capability (e.g. clinical), not a delivery-phase code.
"""

# from __future__ import annotations

from typing import Any


def schema_name(catalog: str, domain: str, layer: str, env: str) -> str:
    catalog = catalog.strip()
    domain = domain.strip().lower().replace("-", "_")
    layer = layer.strip().lower().replace("-", "_")
    env = env.strip().lower()
    return f"{catalog}.{domain}_{layer}_{env}"


def volume_path(catalog: str, schema: str, volume: str, *parts: str) -> str:
    base = f"/Volumes/{catalog.strip()}/{schema.strip()}/{volume.strip()}"
    extra = "/".join(p.strip("/").replace("\\", "/") for p in parts if p)
    return f"{base}/{extra}" if extra else base


def load_uc_yaml(path: str) -> dict[str, Any]:
    """Load config/uc_environment.yaml when PyYAML is available; else minimal parse."""
    try:
        import yaml  # type: ignore
    except ImportError:
        return _minimal_yaml_load(path)
    with open(path, encoding="utf-8") as fh:
        data = yaml.safe_load(fh) or {}
    if not isinstance(data, dict):
        raise ValueError(f"Expected mapping in {path}")
    return data


def _minimal_yaml_load(path: str) -> dict[str, Any]:
    """Tiny subset parser for flat keys used when PyYAML is absent on the cluster."""
    out: dict[str, Any] = {}
    stack: list[tuple[int, dict[str, Any]]] = [(0, out)]
    with open(path, encoding="utf-8") as fh:
        for raw in fh:
            line = raw.split("#", 1)[0].rstrip()
            if not line.strip():
                continue
            indent = len(line) - len(line.lstrip(" "))
            key, _, val = line.lstrip().partition(":")
            key = key.strip()
            val = val.strip().strip('"').strip("'")
            while stack and indent < stack[-1][0]:
                stack.pop()
            parent = stack[-1][1]
            if val == "":
                child: dict[str, Any] = {}
                parent[key] = child
                stack.append((indent + 2, child))
            elif val.lower() in ("true", "false"):
                parent[key] = val.lower() == "true"
            else:
                parent[key] = val
    return out


def resolve_names(cfg: dict[str, Any]) -> dict[str, str]:
    catalog = str(cfg.get("catalog", "hla"))
    env = str(cfg.get("env", "dev"))
    domain = str(cfg.get("domain", "clinical"))
    layers = cfg.get("layers") or {}
    ops = cfg.get("ops") or {}

    def layer(name: str, default: str) -> str:
        return str(layers.get(name, default))

    names = {
        "catalog": catalog,
        "env": env,
        "domain": domain,
        "ref_schema": schema_name(catalog, domain, layer("ref", "ref"), env),
        "bronze_schema": schema_name(catalog, domain, layer("bronze", "bronze"), env),
        "silver_schema": schema_name(catalog, domain, layer("silver", "silver"), env),
        "gold_schema": schema_name(catalog, domain, layer("gold", "gold"), env),
        "sdp_schema": schema_name(catalog, domain, layer("sdp", "sdp"), env),
        "sdp_target": f"{domain}_{layer('sdp', 'sdp')}_{env}",
        "checkpoints_schema": str(ops.get("checkpoints_schema", "ops")),
        "checkpoints_volume": str(ops.get("checkpoints_volume", "checkpoints")),
        "landing_schema": str(ops.get("landing_schema", "landing")),
        # Short UC volumes under landing: ehr + config (not nested clinical/dev/...)
        "landing_ehr_volume": str(
            ops.get("landing_ehr_volume")
            or ops.get("landing_volume")
            or "ehr"
        ),
        "landing_config_volume": str(ops.get("landing_config_volume", "config")),
    }
    # Backward-compatible alias used by older docs/bootstrap widgets
    names["landing_volume"] = names["landing_ehr_volume"]
    names["checkpoint_root_ss"] = volume_path(
        catalog,
        names["checkpoints_schema"],
        names["checkpoints_volume"],
        f"{domain}_spark_ss",
        env,
    )
    names["checkpoint_root_sdp"] = volume_path(
        catalog,
        names["checkpoints_schema"],
        names["checkpoints_volume"],
        f"{domain}_sdp",
        env,
    )
    # Short paths:
    #   /Volumes/{catalog}/landing/ehr/{source}/snapshot|delta
    #   /Volumes/{catalog}/landing/config
    names["landing_ehr_root"] = volume_path(
        catalog,
        names["landing_schema"],
        names["landing_ehr_volume"],
    )
    names["landing_config_root"] = volume_path(
        catalog,
        names["landing_schema"],
        names["landing_config_volume"],
    )
    return names


def landing_ehr_source_path(
    catalog: str,
    source_system_code: str,
    *,
    landing_schema: str = "landing",
    landing_volume: str = "ehr",
) -> str:
    """EHR input root for a source, e.g. /Volumes/hla/landing/ehr/cle."""
    code = source_system_code.strip().lower()
    return volume_path(catalog, landing_schema, landing_volume, code)
