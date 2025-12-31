from pathlib import Path
from typing import Dict, Any
import yaml


def _deep_merge(a: dict, b: dict) -> dict:
    for k, v in b.items():
        if k in a and isinstance(a[k], dict) and isinstance(v, dict):
            _deep_merge(a[k], v)
        else:
            a[k] = v
    return a


def load_cfg(
    mode: str,
    cfg_dir: Path = Path("config"),
) -> Dict[str, Any]:
    base_cfg_path = cfg_dir / "base.yaml"
    if not base_cfg_path.exists():
        raise FileNotFoundError(f"Missing base config: {base_cfg_path}")
    
    experiment_cfg_path = cfg_dir / "experiment.yaml"
    if not experiment_cfg_path.exists():
        raise FileNotFoundError(f"Missing experiment config: {experiment_cfg_path}")

    with open(base_cfg_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    with open(experiment_cfg_path, "r", encoding="utf-8") as f:
        experiment_cfg = yaml.safe_load(f)
        
    cfg = _deep_merge(cfg, experiment_cfg)
    cfg["mode"] = mode
    return cfg
