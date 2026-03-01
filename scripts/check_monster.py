
from pyiceberg.catalog.sql import SqlCatalog
from pathlib import Path

# Use absolute path
db_path = Path("monster_catalog/catalog.db").absolute()
warehouse_path = Path("monster_catalog/warehouse").absolute()

cat = SqlCatalog(
    "monster",
    **{
        "uri": f"sqlite:///{db_path}",
        "warehouse": f"file://{warehouse_path}",
    }
)

print(f"Catalog Tables: {cat.list_tables('bench')}")
print(f"Namespaces: {cat.list_namespaces()}")
