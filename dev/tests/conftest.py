
import pytest
from pathlib import Path
from iceberg_meta.demo import create_demo_catalog, cleanup_demo
from iceberg_meta.catalog import CatalogConfig

@pytest.fixture
def demo_catalog():
    """Create a temporary demo catalog for testing."""
    demo_dir, props = create_demo_catalog()
    config = CatalogConfig(catalog_name="demo", properties=props)
    yield config
    cleanup_demo(demo_dir)
