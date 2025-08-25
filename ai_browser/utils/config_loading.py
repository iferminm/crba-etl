from pathlib import Path
from typing import Any
from box import Box
from ai_browser.config_model import ConfigModel
import yaml
import os



def load_config() -> ConfigModel:
    config_dir = _get_config_dir()
    config_box = _load_yaml_files_from_directory(config_dir)
    return ConfigModel(**config_box)


def _get_config_dir() -> Path:
    config_dir = os.getenv("CONFIG_DIR")
    if config_dir is None:
        raise ValueError("CONFIG_DIR environment variable is not set.")
    return Path(config_dir)


def _load_yaml_files_from_directory(directory_path: Path) -> dict[str, Any]:
    """Load YAML files from a directory."""
    config_box = Box()
    for file_path in directory_path.glob("*.yaml"):
        with open(file_path, "r", encoding="utf-8") as fp:
            config_box |= Box(yaml.safe_load(fp))
    return config_box



config = load_config()