import os
from pathlib import Path
import yaml
import sys

def _find_config() -> Path:
    # Check common Teams/SharePoint sync locations
    home = Path.home()
    
    candidates = [
        home / "Pinnacle" / "P66-AIMI - PCMS to Newton ETL Tool" / "config.yaml",
        home / "OneDrive - Pinnacle" / "P66-AIMI - PCMS to Newton ETL Tool" / "config.yaml",
    ]
    
    for path in candidates:
        if path.exists():
            return path
    
    # Fall back to local (for development)
    if getattr(sys, 'frozen', False):
        return Path(sys.executable).parent / "config.yaml"
    else:
        return Path(__file__).resolve().parent.parent / "config.yaml"

CONFIG_PATH = _find_config()

def load_config() -> dict:
    with CONFIG_PATH.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def save_config(config: dict):
    with CONFIG_PATH.open("w", encoding="utf-8") as f:
        yaml.safe_dump(config, f, sort_keys=False, default_flow_style=False)