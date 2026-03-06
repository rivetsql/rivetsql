"""Tests for ProfileGenerator."""

from __future__ import annotations

from rivet_bridge.profiles import ProfileGenerator
from rivet_config import CatalogConfig, EngineConfig, ResolvedProfile


def _make_profile(
    catalogs: dict[str, CatalogConfig] | None = None,
    engines: list[EngineConfig] | None = None,
    default_engine: str = "arrow",
) -> ResolvedProfile:
    return ResolvedProfile(
        name="test",
        default_engine=default_engine,
        catalogs=catalogs or {},
        engines=engines or [],
    )


class TestProfileGenerator:
    def test_minimal_profile(self):
        profile = _make_profile()
        result = ProfileGenerator().generate(profile)
        assert result.relative_path == "profiles.yaml"
        assert result.joint_name is None
        assert "default_engine: arrow" in result.content

    def test_catalogs_sorted_by_name(self):
        profile = _make_profile(
            catalogs={
                "zeta": CatalogConfig(name="zeta", type="fs", options={}),
                "alpha": CatalogConfig(name="alpha", type="fs", options={}),
            }
        )
        result = ProfileGenerator().generate(profile)
        alpha_pos = result.content.index("    alpha:")
        zeta_pos = result.content.index("    zeta:")
        assert alpha_pos < zeta_pos

    def test_engines_sorted_by_name(self):
        profile = _make_profile(
            engines=[
                EngineConfig(name="zeta_eng", type="arrow", catalogs=[], options={}),
                EngineConfig(name="alpha_eng", type="arrow", catalogs=[], options={}),
            ]
        )
        result = ProfileGenerator().generate(profile)
        alpha_pos = result.content.index("name: alpha_eng")
        zeta_pos = result.content.index("name: zeta_eng")
        assert alpha_pos < zeta_pos

    def test_catalog_options_sorted(self):
        profile = _make_profile(
            catalogs={
                "db": CatalogConfig(name="db", type="pg", options={"host": "localhost", "port": 5432}),
            }
        )
        result = ProfileGenerator().generate(profile)
        host_pos = result.content.index("host:")
        port_pos = result.content.index("port:")
        assert host_pos < port_pos

    def test_engine_with_catalogs_list(self):
        profile = _make_profile(
            engines=[
                EngineConfig(name="duckdb", type="duckdb", catalogs=["main", "staging"], options={}),
            ]
        )
        result = ProfileGenerator().generate(profile)
        assert "catalogs: [main, staging]" in result.content

    def test_credential_sources_placeholder(self):
        profile = _make_profile(
            catalogs={
                "db": CatalogConfig(name="db", type="pg", options={"password": "secret123"}),
            }
        )
        cred_sources = {"catalogs.db.password": "DB_PASSWORD"}
        result = ProfileGenerator().generate(profile, credential_sources=cred_sources)
        assert "${DB_PASSWORD}" in result.content
        assert "secret123" not in result.content

    def test_no_credential_sources_comment(self):
        profile = _make_profile(
            catalogs={
                "db": CatalogConfig(name="db", type="pg", options={"password": "secret123"}),
            }
        )
        result = ProfileGenerator().generate(profile)
        assert "# NOTE: Credential references should be restored manually" in result.content
        assert "secret123" in result.content

    def test_credential_sources_no_comment(self):
        profile = _make_profile(
            catalogs={
                "db": CatalogConfig(name="db", type="pg", options={"password": "secret123"}),
            }
        )
        cred_sources = {"catalogs.db.password": "DB_PASSWORD"}
        result = ProfileGenerator().generate(profile, credential_sources=cred_sources)
        assert "# NOTE:" not in result.content

    def test_full_profile(self):
        profile = _make_profile(
            default_engine="duckdb",
            catalogs={
                "warehouse": CatalogConfig(name="warehouse", type="pg", options={"host": "db.example.com", "port": 5432}),
            },
            engines=[
                EngineConfig(name="duckdb", type="duckdb", catalogs=["warehouse"], options={"threads": 4}),
            ],
        )
        result = ProfileGenerator().generate(profile)
        assert "default_engine: duckdb" in result.content
        assert "catalogs:" in result.content
        assert "engines:" in result.content
        assert "    warehouse:" in result.content
        assert "type: pg" in result.content
        assert "name: duckdb" in result.content
        assert "type: duckdb" in result.content
        assert "catalogs: [warehouse]" in result.content

    def test_engine_options_sorted(self):
        profile = _make_profile(
            engines=[
                EngineConfig(name="eng", type="duckdb", catalogs=[], options={"threads": 4, "memory": "8GB"}),
            ]
        )
        result = ProfileGenerator().generate(profile)
        mem_pos = result.content.index("memory:")
        threads_pos = result.content.index("threads:")
        assert mem_pos < threads_pos
