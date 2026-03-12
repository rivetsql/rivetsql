"""Property tests for RestApiCatalogPlugin introspection.

# Feature: rest-api-catalog, Property 9: list_tables and list_children consistency
# Feature: rest-api-catalog, Property 10: default_table_reference identity
"""

from __future__ import annotations

from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_core.models import Catalog
from rivet_rest.catalog import RestApiCatalogPlugin

# Strategy: generate random endpoint configs with valid paths
_endpoint_name_st = st.text(
    alphabet=st.characters(whitelist_categories=("L", "N"), whitelist_characters="_-"),
    min_size=1,
    max_size=30,
)
_endpoint_cfg_st = st.fixed_dictionaries(
    {"path": st.text(min_size=1, max_size=50).map(lambda s: "/" + s)}
)
_endpoints_st = st.dictionaries(_endpoint_name_st, _endpoint_cfg_st, min_size=0, max_size=10)


def _make_catalog(endpoints: dict[str, dict[str, str]]) -> Catalog:
    return Catalog(
        name="test_api",
        type="rest_api",
        options={
            "base_url": "https://api.example.com",
            "endpoints": endpoints,
        },
    )


@settings(max_examples=100)
@given(endpoints=_endpoints_st)
def test_list_tables_list_children_consistency(endpoints: dict[str, dict[str, str]]) -> None:
    """Property 9: list_children([]) returns the same nodes as list_tables()."""
    plugin = RestApiCatalogPlugin()
    catalog = _make_catalog(endpoints)

    tables = plugin.list_tables(catalog)
    children = plugin.list_children(catalog, [])

    assert len(tables) == len(children)
    table_names = {n.name for n in tables}
    child_names = {n.name for n in children}
    assert table_names == child_names

    # Same node_type for all
    for node in tables:
        assert node.node_type == "endpoint"


@settings(max_examples=100)
@given(name=st.text(min_size=1, max_size=100))
def test_default_table_reference_identity(name: str) -> None:
    """Property 10: default_table_reference returns the name unchanged."""
    plugin = RestApiCatalogPlugin()
    result = plugin.default_table_reference(name, {})
    assert result == name
