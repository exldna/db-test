"""
Initializes the Dynaconf settings object for the data_preloader component.
This module is the single source of truth for all configuration.
"""

from pathlib import Path
from dynaconf import Dynaconf

PROJECT_ROOT = Path(__file__).parent.parent

settings = Dynaconf(
    root_path=PROJECT_ROOT,
    settings_files=["config/settings.toml"],
    secrets=["config/.secrets.toml"],
    envvar_prefix=False,
)
