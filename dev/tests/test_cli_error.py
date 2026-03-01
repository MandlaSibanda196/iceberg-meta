
import pytest
from iceberg_meta.cli import _friendly_error
from iceberg_meta.catalog import CatalogConfig
from pyiceberg.exceptions import (
    NoSuchTableError,
    NoSuchNamespaceError,
    ForbiddenError,
    ServerError,
    NotInstalledError,
    UnauthorizedError
)

def test_friendly_error_no_such_table(capsys):
    config = CatalogConfig("test", {})
    with pytest.raises(SystemExit):
        _friendly_error(NoSuchTableError("Table does not exist"), config, "db.foo")
    
    captured = capsys.readouterr()
    # iceberg-meta uses rich.console(stderr=True) which writes to stderr
    # But pytest capsys captures stderr.
    # rich might detect capture?
    assert "Table not found" in captured.err
    assert "db.foo" in captured.err

def test_friendly_error_auth(capsys):
    config = CatalogConfig("test", {})
    with pytest.raises(SystemExit):
        _friendly_error(UnauthorizedError("401"), config)
    
    captured = capsys.readouterr()
    assert "Authentication failed" in captured.err

def test_friendly_error_generic(capsys):
    config = CatalogConfig("test", {"uri": "foo"})
    with pytest.raises(SystemExit):
        _friendly_error(ValueError("Some random error"), config)
    
    captured = capsys.readouterr()
    assert "Error:" in captured.err
    assert "Some random error" in captured.err
