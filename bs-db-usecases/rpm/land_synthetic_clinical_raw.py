# Databricks notebook source
# MAGIC %md
# MAGIC # Phase3 · Temporary · Land / pace synthetic clinical raw (volume SDP)
# MAGIC
# MAGIC Prepares UC Volume inbox for Autoloader **directory listing** mock ingest.
# MAGIC
# MAGIC | Mode | Behavior |
# MAGIC |---|---|
# MAGIC | `bootstrap` | Create volume + folders only |
# MAGIC | `bulk` | Copy all sample files once (fast smoke test) |
# MAGIC | `paced` | Copy **one file at a time** with sleep (mock realtime) |
# MAGIC
# MAGIC Prefer local `scripts/stream_synthetic_to_landing.py` for laptop→workspace paced upload.
# MAGIC This notebook is for when samples are already synced into the workspace repo.

# COMMAND ----------
dbutils.widgets.text("catalog", "hla")
dbutils.widgets.text("env", "dev")
dbutils.widgets.text("repo_root", "/Workspace/Users/celin.mary@blackstraw.ai/BlackStraw/HLA/RPM/Streaming_Processing")
dbutils.widgets.text("raw_volume_path", "")
dbutils.widgets.dropdown("mode", "paced", ["bootstrap", "bulk", "paced"])
dbutils.widgets.text("interval_seconds", "30")
dbutils.widgets.dropdown("clear_inbox", "false", ["false", "true"])
dbutils.widgets.dropdown("loop", "false", ["false", "true"])

# COMMAND ----------
import time
from datetime import datetime, timezone
from pathlib import Path

CATALOG = dbutils.widgets.get("catalog").strip()
ENV = dbutils.widgets.get("env").strip().lower()
REPO_ROOT = dbutils.widgets.get("repo_root").rstrip("/")
RAW_VOLUME = (
    dbutils.widgets.get("raw_volume_path").strip().rstrip("/")
    or f"/Volumes/{CATALOG}/landing/clinical_raw/{ENV}"
)
INBOX = f"{RAW_VOLUME}/inbox"
SCHEMA_DIR = f"{RAW_VOLUME}/_autoloader_schema"
MODE = dbutils.widgets.get("mode").strip().lower()
INTERVAL = float(dbutils.widgets.get("interval_seconds").strip() or "30")
CLEAR = dbutils.widgets.get("clear_inbox").strip().lower() == "true"
LOOP = dbutils.widgets.get("loop").strip().lower() == "true"

CANDIDATES = [
    Path(f"{REPO_ROOT}/sample_test_data/landing/clinical_raw"),
    Path(f"{REPO_ROOT}/../Phase2/sample_data"),
]
src_root = next((p for p in CANDIDATES if p.exists()), None)

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.landing")
spark.sql(
    f"CREATE VOLUME IF NOT EXISTS {CATALOG}.landing.clinical_raw "
    f"COMMENT 'Clinical raw landing for volume-mode SDP Autoloader (mock ingest)'"
)

for folder in (
    INBOX,
    f"{INBOX}/hl7",
    f"{INBOX}/vendor",
    f"{INBOX}/fhir",
    SCHEMA_DIR,
):
    dbutils.fs.mkdirs(folder)

if CLEAR:
    for sub in ("hl7", "vendor", "fhir"):
        path = f"{INBOX}/{sub}"
        try:
            for f in dbutils.fs.ls(path):
                if f.path.rstrip("/").endswith(("/hl7", "/vendor", "/fhir")):
                    continue
                dbutils.fs.rm(f.path, True)
        except Exception as exc:  # noqa: BLE001
            print(f"[warn] clear {path}: {exc}")

print(f"mode={MODE} inbox={INBOX} interval={INTERVAL}s")

if MODE == "bootstrap":
    dbutils.notebook.exit(f"bootstrap_ok:{INBOX}")

if src_root is None:
    raise FileNotFoundError(
        "No synthetic samples. Sync Phase3 sample_test_data or Phase2 sample_data."
    )


def discover() -> list[tuple[str, Path]]:
    out: list[tuple[str, Path]] = []
    pairs = [
        ("hl7", src_root / "hl7", ("*.hl7", "*.txt")),
        ("vendor", src_root / "vendor", ("*.json",)),
        ("vendor", src_root / "vendor_json", ("*.json",)),
        ("fhir", src_root / "fhir", ("*.json",)),
    ]
    for sub, folder, patterns in pairs:
        if not folder.is_dir():
            continue
        for pattern in patterns:
            for path in sorted(folder.glob(pattern)):
                if path.is_file():
                    out.append((sub, path))
    return out


files = discover()
print(f"source={src_root} files={len(files)}")
if not files:
    raise FileNotFoundError(f"No message files under {src_root}")


def put_one(subdir: str, path: Path, stamp: str, idx: int) -> str:
    dest = f"{INBOX}/{subdir}/{stamp}_{idx:04d}_{path.name}"
    dbutils.fs.put(
        dest,
        path.read_text(encoding="utf-8", errors="replace"),
        overwrite=True,
    )
    return dest


copied: list[str] = []
pass_no = 0
while True:
    pass_no += 1
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    print(f"=== pass {pass_no} stamp={stamp} ===")
    for idx, (sub, path) in enumerate(files):
        dest = put_one(sub, path, stamp, idx)
        copied.append(dest)
        print(f"[{idx + 1}/{len(files)}] {dest}")
        if MODE == "paced" and idx < len(files) - 1 and INTERVAL > 0:
            time.sleep(INTERVAL)
    if MODE == "bulk" or not LOOP:
        break
    if INTERVAL > 0:
        time.sleep(INTERVAL)

display(spark.createDataFrame([(p,) for p in copied], ["landed_path"]))
print(f"done files_landed={len(copied)} inbox={INBOX}")

