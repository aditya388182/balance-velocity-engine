from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any, Dict

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG = REPO_ROOT / "conf" / "engine_config.yml"

_DURATION_RE = re.compile(
    r"^\s*(\d+(?:\.\d+)?)\s*(ms|milliseconds?|s|sec|secs|seconds?|m|min|mins|minutes?|h|hours?|d|days?)\s*$"
)
_UNIT_MS = {
    "ms": 1, "millisecond": 1, "milliseconds": 1,
    "s": 1000, "sec": 1000, "secs": 1000, "second": 1000, "seconds": 1000,
    "m": 60_000, "min": 60_000, "mins": 60_000, "minute": 60_000, "minutes": 60_000,
    "h": 3_600_000, "hour": 3_600_000, "hours": 3_600_000,
    "d": 86_400_000, "day": 86_400_000, "days": 86_400_000,
}


def duration_ms(text: str) -> int:
    """'30 seconds' -> 30000. Used by the TTL comparison and the Day-3 timing probe."""
    m = _DURATION_RE.match(str(text))
    if not m:
        raise ValueError(f"unparseable duration: {text!r}")
    return int(float(m.group(1)) * _UNIT_MS[m.group(2)])


# env var -> (dotted path into the config dict, coercion)
_ENV_OVERRIDES = {
    "P3_MAX_BUFFER_SIZE": ("max_buffer_size", int),
    "P3_WATERMARK_DELAY": ("watermark_delay", str),
    "P3_TRIGGER_INTERVAL": ("trigger_interval", str),
    "P3_STATE_TTL": ("state_ttl", str),
    "P3_GAP_POLICY": ("gap_policy", str),
    "P3_CHECKPOINT_ROOT": ("checkpoint_root", str),
    "P3_SPARK_VERSION_TAG": ("spark_version_tag", str),
    "P3_KAFKA_BOOTSTRAP": ("kafka_bootstrap", str),
    "P3_SCHEMA_REGISTRY_URL": ("schema_registry_url", str),
    "P3_S3_ENDPOINT": ("s3.endpoint", str),
    "P3_SHUFFLE_PARTITIONS": ("spark.shuffle_partitions", int),
    "P3_MASTER": ("spark.master", str),
    "P3_MAX_OFFSETS_PER_TRIGGER": ("spark.max_offsets_per_trigger", int),
    "P3_VEL_LIMIT": ("vel_limit", int),
}


def _get_dotted(cfg: Dict[str, Any], dotted: str) -> Any:
    node: Any = cfg
    for part in dotted.split("."):
        node = node[part]
    return node


def _set_dotted(cfg: Dict[str, Any], dotted: str, value: Any) -> None:
    parts = dotted.split(".")
    node: Any = cfg
    for part in parts[:-1]:
        node = node[part]
    node[parts[-1]] = value


def _apply_env_overrides(cfg: Dict[str, Any], announce: bool = True) -> Dict[str, Any]:
    applied = []
    for env_key, (dotted, coerce) in _ENV_OVERRIDES.items():
        raw = os.environ.get(env_key)
        if raw is None or raw == "":
            continue
        before = _get_dotted(cfg, dotted)
        after = coerce(raw)
        _set_dotted(cfg, dotted, after)
        applied.append(f"{dotted}: {before!r} -> {after!r}  (via {env_key})")
    cfg["_env_overrides"] = applied
    if applied and announce and os.environ.get("P3_QUIET_CONFIG") != "1":
        # Printed on every run so a screenshot of a drill is self-documenting:
        # you can always see which parameters that run actually used.
        print("[config] environment overrides active:")
        for line in applied:
            print(f"[config]   {line}")
    return cfg


def load_config(path: str | os.PathLike | None = None, announce: bool = True) -> Dict[str, Any]:
    """Load engine_config.yml into the plain dict the whole project reads as CFG."""
    cfg_path = Path(path) if path else DEFAULT_CONFIG
    with open(cfg_path, "r") as fh:
        cfg = yaml.safe_load(fh)
    cfg["_config_path"] = str(cfg_path)
    cfg = _apply_env_overrides(cfg, announce=announce)
    return _derive(cfg)


def _derive(cfg: Dict[str, Any]) -> Dict[str, Any]:
    """Pre-parsed millisecond forms of the duration strings.

    The pure sequencer core must not depend on a duration parser, and it must not
    re-parse a string on every micro-batch. Deriving these once at load time keeps
    step() taking a plain dict of numbers, which is also what makes it trivial to
    construct a config in a unit test.
    """
    cfg["watermark_delay_ms"] = duration_ms(cfg["watermark_delay"])
    cfg["trigger_interval_ms"] = duration_ms(cfg["trigger_interval"])
    cfg["state_ttl_ms"] = duration_ms(cfg["state_ttl"])
    cfg["gap_realert_ms"] = duration_ms(cfg.get("gap_realert_interval", "60 seconds"))
    return cfg


def checkpoint_path(cfg: Dict[str, Any], query: str = "balance_engine") -> str:
    """f"{checkpoint_root}/{spark_version_tag}/{query}" — built here and nowhere else."""
    return f'{cfg["checkpoint_root"]}/{cfg["spark_version_tag"]}/{query}'


CFG = load_config()
