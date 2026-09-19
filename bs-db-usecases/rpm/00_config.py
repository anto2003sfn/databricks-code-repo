# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Phase3 · SDP shared config (interactive shim)
# MAGIC
# MAGIC Real config lives in **`SDP/sdp_config.py`** (a plain Python module) because
# MAGIC Declarative Pipelines ignore `%run`. Pipeline notebooks `01` and `02` import
# MAGIC that module directly.
# MAGIC
# MAGIC This notebook exists only for interactive/ad-hoc use:
# MAGIC `%run ./00_config` from a normal notebook still exposes the same names.

# COMMAND ----------

import importlib.util
import os
import sys
from pathlib import Path


def load_sdp_config():
    candidates = []
    for key in ("clinical.repo_root", "rpm.databricks-code-repo"):
        try:
            value = spark.conf.get(key)
        except Exception:
            value = None
        if value and value.strip():
            candidates.append(value.strip())
    here = Path(os.getcwd())
    candidates += [str(here), str(here.parent), "/Workspace/Users/anto2003.sfn@gmail.com/databricks-code-repo/bs-db-usecases/rpm/"]
    for raw in candidates:
        root = raw.rstrip("/").replace("\\", "/")
        if root.startswith(("/Repos/", "/Users/", "/Shared/")):
            root = f"/Workspace{root}"
        module_file = Path(root, "rpm", "sdp_config.py")
        if module_file.is_file():
            if root not in sys.path:
                sys.path.insert(0, root)
            spec = importlib.util.spec_from_file_location(
                "rpm_config", str(module_file)
            )
            module = importlib.util.module_from_spec(spec)
            sys.modules[spec.name] = module
            spec.loader.exec_module(module)
            print('candidate',candidates)
            return module
    raise RuntimeError(
        "rpm/sdp_config.py not found (it must exist as a workspace FILE, not a "
        "notebook). Set clinical.repo_root to the folder that contains config/ "
        "and rpm/. Looked under: " + ", ".join(candidates)
    )


cfg = load_sdp_config()
print('cfg',cfg)
globals().update({name: getattr(cfg, name) for name in cfg.__all__})

# COMMAND ----------


