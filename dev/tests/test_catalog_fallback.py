
import pytest
from unittest.mock import patch, mock_open
from iceberg_meta.catalog import resolve_catalog_config, CONFIG_FILE, CatalogConfig

def test_resolve_fallback_when_config_exists():
    """
    If ~/.iceberg-meta.yaml exists but the requested catalog is NOT in it,
    we should fall back to a default config (letting pyiceberg handle it)
    instead of raising ValueError.
    """
    yaml_content = """
    catalogs:
      local_demo:
        uri: sqlite:///demo.db
    """
    
    with patch("builtins.open", mock_open(read_data=yaml_content)):
        with patch("pathlib.Path.exists", return_value=True):
            # This should NOT raise ValueError now
            config = resolve_catalog_config(catalog_name="aws_prod")
            
            # It should return a config with the name "aws_prod" and essentially empty properties
            # (except for env overrides)
            assert config.catalog_name == "aws_prod"
            # It should NOT have the local_demo properties
            assert "uri" not in config.properties

def test_resolve_explicit_in_config():
    yaml_content = """
    catalogs:
      local_demo:
        uri: sqlite:///demo.db
    """
    
    with patch("builtins.open", mock_open(read_data=yaml_content)):
        with patch("pathlib.Path.exists", return_value=True):
            config = resolve_catalog_config(catalog_name="local_demo")
            assert config.catalog_name == "local_demo"
            assert config.properties["uri"] == "sqlite:///demo.db"
