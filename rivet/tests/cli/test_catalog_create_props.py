"""Property-based tests for catalog creation wizard pure helpers.

Task 1.2: Property tests for name validation (Properties 1, 2, 3)
Task 1.3: Property tests for option handling (Properties 4, 5)
Task 1.4: Property tests for credential and preview helpers (Properties 6, 7, 8, 9)

Properties verified:
- Property 1: validate_catalog_name accepts only valid names
- Property 2: validate_catalog_name detects conflicts
- Property 3: suggest_default_name derives correct default
- Property 4: Missing required options detection
- Property 5: Declining optional settings uses all defaults
- Property 6: Environment variable name suggestion
- Property 7: Plaintext credential detection
- Property 8: Catalog block YAML round-trip
- Property 9: Credential masking in preview

Validates: Requirements 4.2, 4.3, 4.6, 2.8, 6.4, 7.3, 7.4, 10.1, 10.2
"""

from __future__ import annotations

import yaml
from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_cli.commands.catalog_create import (
    CATALOG_NAME_MAX_LEN,
    WizardState,
    build_catalog_block,
    find_missing_required_options,
    is_env_var_ref,
    mask_credentials_for_preview,
    merge_optional_defaults,
    suggest_default_name,
    suggest_env_var_name,
    validate_catalog_name,
)

# ── Strategies ─────────────────────────────────────────────────────────────────

_valid_name = st.from_regex(r"[a-z][a-z0-9_]{0,63}", fullmatch=True).filter(
    lambda s: len(s) <= CATALOG_NAME_MAX_LEN
)

_invalid_name = st.one_of(
    st.just(""),
    # starts with digit
    st.from_regex(r"[0-9][a-z0-9_]*", fullmatch=True),
    # starts with uppercase
    st.from_regex(r"[A-Z][a-zA-Z0-9_]*", fullmatch=True),
    # contains invalid chars (hyphen, dot, space)
    st.from_regex(r"[a-z][a-z0-9_]*[-. ][a-z0-9_]*", fullmatch=True),
    # too long (65+ chars)
    st.text(alphabet="abcdefghijklmnopqrstuvwxyz0123456789_", min_size=65).filter(
        lambda s: s and s[0].islower()
    ),
)

_catalog_name = st.from_regex(r"[a-z][a-z0-9_]{0,30}", fullmatch=True)
_option_name = st.from_regex(r"[a-z][a-z0-9_]{0,20}", fullmatch=True)
_plaintext = st.text(min_size=1).filter(lambda v: not (v.startswith("${") and v.endswith("}")))
_env_ref = st.from_regex(r"\$\{[A-Z][A-Z0-9_]*\}", fullmatch=True)
_scalar = st.one_of(st.text(min_size=0, max_size=50), st.integers(), st.booleans())


# ── Property 1: Catalog name validation accepts only valid names ───────────────
# Feature: catalog-creation-wizard, Property 1: Catalog name validation accepts only valid names


@given(_valid_name)
@settings(max_examples=100)
def test_valid_names_pass_validation(name: str) -> None:
    """Any name matching ^[a-z][a-z0-9_]*$ with len ≤ 64 must be accepted."""
    assert validate_catalog_name(name, set()) is None


@given(_invalid_name)
@settings(max_examples=100)
def test_invalid_names_fail_validation(name: str) -> None:
    """Any name not matching the pattern or exceeding max length must be rejected."""
    result = validate_catalog_name(name, set())
    assert result is not None
    assert len(result) > 0


# ── Property 2: Catalog name conflict detection ────────────────────────────────
# Feature: catalog-creation-wizard, Property 2: Catalog name conflict detection


@given(_valid_name, st.frozensets(_valid_name, min_size=1, max_size=10))
@settings(max_examples=100)
def test_conflict_detected_when_name_in_existing(name: str, existing: frozenset[str]) -> None:
    """A valid name that is in existing_names must produce a conflict error."""
    existing_with_name = existing | {name}
    result = validate_catalog_name(name, set(existing_with_name))
    assert result is not None
    assert len(result) > 0


@given(_valid_name, st.frozensets(_valid_name, min_size=0, max_size=10))
@settings(max_examples=100)
def test_no_conflict_when_name_not_in_existing(name: str, existing: frozenset[str]) -> None:
    """A valid name absent from existing_names must pass validation."""
    existing_without_name = existing - {name}
    assert validate_catalog_name(name, set(existing_without_name)) is None


# ── Property 3: Default catalog name derivation ────────────────────────────────
# Feature: catalog-creation-wizard, Property 3: Default catalog name derivation


@given(st.from_regex(r"[a-z][a-z0-9_]{0,59}", fullmatch=True))
@settings(max_examples=100)
def test_default_name_equals_my_prefix(catalog_type: str) -> None:
    """suggest_default_name must return f'my_{catalog_type}'."""
    assert suggest_default_name(catalog_type) == f"my_{catalog_type}"


@given(st.from_regex(r"[a-z][a-z0-9_]{0,59}", fullmatch=True))
@settings(max_examples=100)
def test_default_name_passes_validation(catalog_type: str) -> None:
    """The suggested default name must itself pass name validation."""
    default = suggest_default_name(catalog_type)
    assert validate_catalog_name(default, set()) is None


# ---------------------------------------------------------------------------
# Property 4: Missing required options detection
# Validates: Requirements 2.8, 13.2
# ---------------------------------------------------------------------------


class TestProperty4MissingRequiredOptions:
    """Property 4: find_missing_required_options returns exactly the set difference."""

    @given(
        required=st.lists(st.text(min_size=1, max_size=20), min_size=0, max_size=10, unique=True),
        provided=st.lists(st.text(min_size=1, max_size=20), min_size=0, max_size=10, unique=True),
    )
    @settings(max_examples=200)
    def test_missing_is_set_difference(self, required: list[str], provided: list[str]) -> None:
        # Feature: catalog-creation-wizard, Property 4: Missing required options detection
        result = find_missing_required_options(required, set(provided))
        expected = [opt for opt in required if opt not in provided]
        assert result == expected

    @given(
        required=st.lists(st.text(min_size=1, max_size=20), min_size=1, max_size=10, unique=True),
    )
    @settings(max_examples=100)
    def test_all_provided_returns_empty(self, required: list[str]) -> None:
        result = find_missing_required_options(required, set(required))
        assert result == []

    @given(
        required=st.lists(st.text(min_size=1, max_size=20), min_size=1, max_size=10, unique=True),
    )
    @settings(max_examples=100)
    def test_none_provided_returns_all(self, required: list[str]) -> None:
        result = find_missing_required_options(required, set())
        assert result == required

    @given(
        required=st.lists(st.text(min_size=1, max_size=20), min_size=2, max_size=10, unique=True),
    )
    @settings(max_examples=100)
    def test_strict_subset_provided_returns_remainder(self, required: list[str]) -> None:
        # Provide only the first half; the rest should be reported missing
        half = len(required) // 2
        provided = set(required[:half])
        result = find_missing_required_options(required, provided)
        assert set(result) == set(required) - provided

    def test_empty_required_always_empty(self) -> None:
        assert find_missing_required_options([], set()) == []
        assert find_missing_required_options([], {"a", "b"}) == []

    def test_order_preserved(self) -> None:
        required = ["c", "a", "b"]
        result = find_missing_required_options(required, {"a"})
        assert result == ["c", "b"]


# ---------------------------------------------------------------------------
# Property 5: Declining optional settings uses all defaults
# Validates: Requirements 6.4
# ---------------------------------------------------------------------------

_simple_value = st.one_of(
    st.text(max_size=20),
    st.integers(),
    st.booleans(),
    st.none(),
)


class TestProperty5MergeOptionalDefaults:
    """Property 5: merge_optional_defaults fills missing keys with defaults."""

    @given(
        optional_options=st.dictionaries(
            st.text(min_size=1, max_size=10),
            _simple_value,
            max_size=8,
        ),
        user_provided=st.dictionaries(
            st.text(min_size=1, max_size=10),
            _simple_value,
            max_size=8,
        ),
    )
    @settings(max_examples=200)
    def test_all_defaults_present_when_user_provides_nothing(
        self,
        optional_options: dict,
        user_provided: dict,
    ) -> None:
        # Feature: catalog-creation-wizard, Property 5: Declining optional settings uses all defaults
        result = merge_optional_defaults(optional_options, user_provided)
        # Every non-None key from optional_options must appear in result
        for key in optional_options:
            if optional_options[key] is not None:
                assert key in result

    @given(
        optional_options=st.dictionaries(
            st.text(min_size=1, max_size=10),
            _simple_value,
            max_size=8,
        ),
    )
    @settings(max_examples=100)
    def test_declining_all_returns_exact_defaults(self, optional_options: dict) -> None:
        # When user provides nothing, result equals the non-None defaults
        result = merge_optional_defaults(optional_options, {})
        expected = {k: v for k, v in optional_options.items() if v is not None}
        assert result == expected

    @given(
        optional_options=st.dictionaries(
            st.text(min_size=1, max_size=10),
            _simple_value,
            max_size=8,
        ),
        user_provided=st.dictionaries(
            st.text(min_size=1, max_size=10),
            _simple_value,
            max_size=8,
        ),
    )
    @settings(max_examples=200)
    def test_user_values_override_defaults(
        self,
        optional_options: dict,
        user_provided: dict,
    ) -> None:
        result = merge_optional_defaults(optional_options, user_provided)
        # User-provided values always win
        for key, value in user_provided.items():
            assert result[key] == value

    @given(
        optional_options=st.dictionaries(
            st.text(min_size=1, max_size=10),
            _simple_value,
            min_size=1,
            max_size=8,
        ),
    )
    @settings(max_examples=100)
    def test_defaults_fill_missing_keys(self, optional_options: dict) -> None:
        # Provide only a subset of keys; missing ones should come from non-None defaults
        keys = list(optional_options.keys())
        half = len(keys) // 2
        user_provided = {k: optional_options[k] for k in keys[:half]}
        result = merge_optional_defaults(optional_options, user_provided)
        for key in keys[half:]:
            if optional_options[key] is not None:
                assert result[key] == optional_options[key]

    def test_empty_optional_empty_user(self) -> None:
        assert merge_optional_defaults({}, {}) == {}

    def test_empty_optional_with_user(self) -> None:
        result = merge_optional_defaults({}, {"extra": "val"})
        assert result == {"extra": "val"}


# ── Property 6: Environment variable name suggestion ──────────────────
# Feature: catalog-creation-wizard, Property 6: Environment variable name suggestion


@given(catalog_name=_catalog_name, option_name=_option_name)
def test_suggest_env_var_name_format(catalog_name: str, option_name: str) -> None:
    """suggest_env_var_name always returns a ${UPPER_UPPER} reference."""
    result = suggest_env_var_name(catalog_name, option_name)
    assert result.startswith("${")
    assert result.endswith("}")
    inner = result[2:-1]
    assert inner == inner.upper()
    assert inner == f"{catalog_name.upper()}_{option_name.upper()}"


@given(catalog_name=_catalog_name, option_name=_option_name)
def test_suggest_env_var_name_is_env_var_ref(catalog_name: str, option_name: str) -> None:
    """suggest_env_var_name output is always recognised by is_env_var_ref."""
    result = suggest_env_var_name(catalog_name, option_name)
    assert is_env_var_ref(result)


# ── Property 7: Plaintext credential detection ────────────────────────
# Feature: catalog-creation-wizard, Property 7: Plaintext credential detection


@given(value=_plaintext)
def test_is_env_var_ref_rejects_plaintext(value: str) -> None:
    """is_env_var_ref returns False for any non-${...} string."""
    assert not is_env_var_ref(value)


@given(value=_env_ref)
def test_is_env_var_ref_accepts_env_refs(value: str) -> None:
    """is_env_var_ref returns True for any ${...} pattern."""
    assert is_env_var_ref(value)


# ── Property 8: Catalog block YAML round-trip ─────────────────────────
# Feature: catalog-creation-wizard, Property 8: Catalog block YAML round-trip


@given(
    catalog_type=st.from_regex(r"[a-z][a-z0-9_]{1,15}", fullmatch=True),
    required=st.dictionaries(
        st.from_regex(r"[a-z][a-z0-9_]{1,10}", fullmatch=True),
        st.text(min_size=1, max_size=30),
        max_size=4,
    ),
    optional=st.dictionaries(
        st.from_regex(r"[a-z][a-z0-9_]{1,10}", fullmatch=True),
        _scalar,
        max_size=4,
    ),
    credentials=st.dictionaries(
        st.from_regex(r"[a-z][a-z0-9_]{1,10}", fullmatch=True),
        st.text(min_size=1, max_size=30),
        max_size=3,
    ),
)
def test_build_catalog_block_yaml_round_trip(
    catalog_type: str,
    required: dict,
    optional: dict,
    credentials: dict,
) -> None:
    """build_catalog_block output survives a YAML dump/load round-trip unchanged."""
    state = WizardState(
        catalog_type=catalog_type,
        catalog_name="test_catalog",
        required_opts=required,
        optional_opts=optional,
        credential_opts=credentials,
    )
    block = build_catalog_block(state)
    dumped = yaml.dump(block, default_flow_style=False)
    loaded = yaml.safe_load(dumped)
    assert loaded == block


@given(
    catalog_type=st.from_regex(r"[a-z][a-z0-9_]{1,15}", fullmatch=True),
)
def test_build_catalog_block_always_has_type(catalog_type: str) -> None:
    """build_catalog_block always includes the 'type' key."""
    state = WizardState(catalog_type=catalog_type, catalog_name="x")
    block = build_catalog_block(state)
    assert block["type"] == catalog_type


# ── Property 9: Credential masking in preview ─────────────────────────
# Feature: catalog-creation-wizard, Property 9: Credential masking in preview


@given(
    catalog_name=_catalog_name,
    cred_keys=st.lists(
        st.from_regex(r"[a-z][a-z0-9_]{1,10}", fullmatch=True),
        min_size=1,
        max_size=4,
        unique=True,
    ),
    plaintext_values=st.lists(
        _plaintext,
        min_size=1,
        max_size=4,
    ),
)
def test_mask_credentials_replaces_plaintext(
    catalog_name: str,
    cred_keys: list[str],
    plaintext_values: list[str],
) -> None:
    """mask_credentials_for_preview replaces all plaintext creds with ${ENV_VAR} refs."""
    block = {"type": "test"}
    for key, val in zip(cred_keys, plaintext_values):
        block[key] = val

    masked = mask_credentials_for_preview(block, cred_keys, catalog_name)

    for key in cred_keys:
        if key in block:
            assert is_env_var_ref(str(masked[key])), (
                f"Expected env-var ref for key '{key}', got: {masked[key]!r}"
            )


@given(
    catalog_name=_catalog_name,
    cred_keys=st.lists(
        st.from_regex(r"[a-z][a-z0-9_]{1,10}", fullmatch=True),
        min_size=1,
        max_size=4,
        unique=True,
    ),
    env_values=st.lists(
        _env_ref,
        min_size=1,
        max_size=4,
    ),
)
def test_mask_credentials_preserves_existing_env_refs(
    catalog_name: str,
    cred_keys: list[str],
    env_values: list[str],
) -> None:
    """mask_credentials_for_preview leaves existing ${ENV_VAR} refs unchanged."""
    block = {"type": "test"}
    for key, val in zip(cred_keys, env_values):
        block[key] = val

    masked = mask_credentials_for_preview(block, cred_keys, catalog_name)

    for key, val in zip(cred_keys, env_values):
        if key in block:
            assert masked[key] == val, (
                f"Expected existing env-ref preserved for key '{key}'"
            )


@given(
    catalog_name=_catalog_name,
    non_cred_keys=st.lists(
        st.from_regex(r"[a-z][a-z0-9_]{1,10}", fullmatch=True),
        min_size=1,
        max_size=4,
        unique=True,
    ),
    values=st.lists(st.text(min_size=1, max_size=20), min_size=1, max_size=4),
)
def test_mask_credentials_does_not_touch_non_credential_keys(
    catalog_name: str,
    non_cred_keys: list[str],
    values: list[str],
) -> None:
    """mask_credentials_for_preview leaves non-credential keys untouched."""
    block = {"type": "test"}
    for key, val in zip(non_cred_keys, values):
        block[key] = val

    # Pass empty credential_keys so nothing should be masked
    masked = mask_credentials_for_preview(block, [], catalog_name)

    for key, val in zip(non_cred_keys, values):
        if key in block:
            assert masked[key] == val


# ── Property 10: Profile write preserves existing catalogs and handles overwrite ──
# Feature: catalog-creation-wizard, Property 10: Profile write preserves existing catalogs and handles overwrite

import tempfile
from pathlib import Path

from rivet_cli.commands.catalog_create import write_catalog_to_profile

_catalog_type = st.from_regex(r"[a-z][a-z0-9_]{1,10}", fullmatch=True)
_catalog_block = st.fixed_dictionaries({"type": _catalog_type})


@given(
    existing_names=st.lists(
        st.from_regex(r"[a-z][a-z0-9_]{1,15}", fullmatch=True),
        min_size=1,
        max_size=5,
        unique=True,
    ),
    new_name=st.from_regex(r"[a-z][a-z0-9_]{1,15}", fullmatch=True),
    new_block=_catalog_block,
)
@settings(max_examples=100)
def test_write_preserves_existing_catalogs(
    existing_names: list[str],
    new_name: str,
    new_block: dict,
) -> None:
    """Writing a new catalog leaves all other existing catalogs unchanged."""
    # Feature: catalog-creation-wizard, Property 10: Profile write preserves existing catalogs and handles overwrite
    with tempfile.TemporaryDirectory() as tmp:
        profiles = Path(tmp) / "profiles.yaml"
        existing_blocks = {name: {"type": "duckdb", "path": f"./{name}"} for name in existing_names}
        profiles.write_text(yaml.dump({"default": {"catalogs": dict(existing_blocks)}}))

        write_catalog_to_profile(profiles, "default", new_name, new_block, None)

        data = yaml.safe_load(profiles.read_text())
        catalogs = data["default"]["catalogs"]
        assert catalogs[new_name] == new_block
        for name, block in existing_blocks.items():
            if name != new_name:
                assert catalogs[name] == block


@given(
    catalog_name=st.from_regex(r"[a-z][a-z0-9_]{1,15}", fullmatch=True),
    old_block=_catalog_block,
    new_block=_catalog_block,
)
@settings(max_examples=100)
def test_write_overwrites_existing_catalog(
    catalog_name: str,
    old_block: dict,
    new_block: dict,
) -> None:
    """Writing to an existing catalog name fully replaces the old block."""
    # Feature: catalog-creation-wizard, Property 10: Profile write preserves existing catalogs and handles overwrite
    with tempfile.TemporaryDirectory() as tmp:
        profiles = Path(tmp) / "profiles.yaml"
        profiles.write_text(yaml.dump({"default": {"catalogs": {catalog_name: old_block}}}))

        write_catalog_to_profile(profiles, "default", catalog_name, new_block, None)

        data = yaml.safe_load(profiles.read_text())
        assert data["default"]["catalogs"][catalog_name] == new_block


# ── Property 11: Engine compatibility filtering ────────────────────────────────
# Feature: catalog-creation-wizard, Property 11: Engine compatibility filtering

from rivet_cli.commands.catalog_create import filter_compatible_engines
from rivet_config.models import EngineConfig, ResolvedProfile
from rivet_core.plugins import ComputeEngineAdapter, ComputeEnginePlugin, PluginRegistry


def _make_adapter_for_prop(engine_type: str, catalog_type: str) -> ComputeEngineAdapter:
    A = type(
        "A",
        (ComputeEngineAdapter,),
        {
            "target_engine_type": engine_type,
            "catalog_type": catalog_type,
            "capabilities": ["read"],
            "source": "engine_plugin",
            "read_dispatch": lambda self, e, c, j: None,
            "write_dispatch": lambda self, e, c, j, m: None,
        },
    )
    return A()


def _make_engine_plugin_for_prop(etype: str, supported: dict) -> ComputeEnginePlugin:
    class P(ComputeEnginePlugin):
        engine_type = etype
        supported_catalog_types = supported

        def create_engine(self, n, c): ...
        def validate(self, o): ...
        def execute_sql(self, engine, sql, input_tables):
            raise NotImplementedError

    return P()


_engine_type_st = st.from_regex(r"[a-z][a-z0-9]{2,8}", fullmatch=True)
_catalog_type_st = st.from_regex(r"[a-z][a-z0-9]{2,8}", fullmatch=True)


@given(
    engine_types=st.lists(_engine_type_st, min_size=1, max_size=5, unique=True),
    catalog_type=_catalog_type_st,
)
@settings(max_examples=100)
def test_filter_returns_only_compatible_engines_via_adapter(
    engine_types: list[str],
    catalog_type: str,
) -> None:
    """filter_compatible_engines returns exactly engines with a registered adapter."""
    # Feature: catalog-creation-wizard, Property 11: Engine compatibility filtering
    registry = PluginRegistry()
    # Register adapter only for the first engine type
    compatible_type = engine_types[0]
    registry.register_adapter(_make_adapter_for_prop(compatible_type, catalog_type))

    engines = [EngineConfig(f"e{i}", et, [], {}) for i, et in enumerate(engine_types)]
    profile = ResolvedProfile("default", engines[0].name, {}, engines)

    result = filter_compatible_engines(profile, catalog_type, registry)
    result_types = {e.type for e in result}
    assert compatible_type in result_types
    for et in engine_types[1:]:
        assert et not in result_types


@given(
    engine_types=st.lists(_engine_type_st, min_size=1, max_size=5, unique=True),
    catalog_type=_catalog_type_st,
)
@settings(max_examples=100)
def test_filter_returns_only_compatible_engines_via_plugin(
    engine_types: list[str],
    catalog_type: str,
) -> None:
    """filter_compatible_engines returns exactly engines whose plugin supports the catalog type."""
    # Feature: catalog-creation-wizard, Property 11: Engine compatibility filtering
    registry = PluginRegistry()
    compatible_type = engine_types[0]
    registry.register_engine_plugin(
        _make_engine_plugin_for_prop(compatible_type, {catalog_type: ["read"]})
    )

    engines = [EngineConfig(f"e{i}", et, [], {}) for i, et in enumerate(engine_types)]
    profile = ResolvedProfile("default", engines[0].name, {}, engines)

    result = filter_compatible_engines(profile, catalog_type, registry)
    result_types = {e.type for e in result}
    assert compatible_type in result_types
    for et in engine_types[1:]:
        assert et not in result_types


@given(
    engine_types=st.lists(_engine_type_st, min_size=1, max_size=5, unique=True),
    catalog_type=_catalog_type_st,
)
@settings(max_examples=100)
def test_filter_empty_registry_returns_no_engines(
    engine_types: list[str],
    catalog_type: str,
) -> None:
    """With no adapters or plugins registered, no engines are compatible."""
    # Feature: catalog-creation-wizard, Property 11: Engine compatibility filtering
    registry = PluginRegistry()
    engines = [EngineConfig(f"e{i}", et, [], {}) for i, et in enumerate(engine_types)]
    profile = ResolvedProfile("default", engines[0].name, {}, engines)

    result = filter_compatible_engines(profile, catalog_type, registry)
    assert result == []


# ── Property 12: Engine catalog list update ───────────────────────────────────
# Feature: catalog-creation-wizard, Property 12: Engine catalog list update

from rivet_cli.commands.catalog_create import update_engine_catalogs

_catalog_name_st = st.from_regex(r"[a-z][a-z0-9_]{1,15}", fullmatch=True)


@given(
    existing=st.lists(_catalog_name_st, min_size=0, max_size=10),
    new_name=_catalog_name_st,
)
@settings(max_examples=200)
def test_update_engine_catalogs_contains_new_name(
    existing: list[str],
    new_name: str,
) -> None:
    """After update, the catalog name is always present in the result."""
    # Feature: catalog-creation-wizard, Property 12: Engine catalog list update
    result = update_engine_catalogs(existing, new_name)
    assert new_name in result


@given(
    existing=st.lists(_catalog_name_st, min_size=0, max_size=10, unique=True),
    new_name=_catalog_name_st,
)
@settings(max_examples=200)
def test_update_engine_catalogs_no_duplicates(
    existing: list[str],
    new_name: str,
) -> None:
    """When the existing list has no duplicates, the result contains new_name exactly once."""
    # Feature: catalog-creation-wizard, Property 12: Engine catalog list update
    result = update_engine_catalogs(existing, new_name)
    assert result.count(new_name) == 1


@given(
    existing=st.lists(_catalog_name_st, min_size=1, max_size=10),
    new_name=_catalog_name_st,
)
@settings(max_examples=200)
def test_update_engine_catalogs_idempotent(
    existing: list[str],
    new_name: str,
) -> None:
    """Calling update twice produces the same result as calling it once."""
    # Feature: catalog-creation-wizard, Property 12: Engine catalog list update
    once = update_engine_catalogs(existing, new_name)
    twice = update_engine_catalogs(once, new_name)
    assert once == twice


@given(
    existing=st.lists(_catalog_name_st, min_size=1, max_size=10),
    new_name=_catalog_name_st,
)
@settings(max_examples=200)
def test_update_engine_catalogs_preserves_existing(
    existing: list[str],
    new_name: str,
) -> None:
    """All pre-existing catalog names are preserved in the result."""
    # Feature: catalog-creation-wizard, Property 12: Engine catalog list update
    result = update_engine_catalogs(existing, new_name)
    for name in existing:
        assert name in result


# ── Property 13: All wizard errors have message and remediation ───────────────
# Feature: catalog-creation-wizard, Property 13: All wizard errors have message and remediation
# Validates: Requirement 13.8

from rivet_cli.errors import (
    RVT_850,
    RVT_880,
    RVT_881,
    RVT_882,
    RVT_883,
    RVT_884,
    RVT_885,
    RVT_886,
    CLIError,
)

_nonempty_text = st.text(min_size=1, max_size=80)
_wizard_error_codes = st.sampled_from([RVT_850, RVT_880, RVT_881, RVT_882, RVT_883, RVT_884, RVT_885, RVT_886])


def _has_nonempty_message_and_remediation(err: CLIError) -> bool:
    return bool(err.message) and bool(err.remediation)


@given(code=_wizard_error_codes, message=_nonempty_text, remediation=_nonempty_text)
@settings(max_examples=200)
def test_cli_error_message_and_remediation_nonempty(code: str, message: str, remediation: str) -> None:
    """Any CLIError with a wizard error code must have non-empty message and remediation."""
    # Feature: catalog-creation-wizard, Property 13: All wizard errors have message and remediation
    err = CLIError(code=code, message=message, remediation=remediation)
    assert _has_nonempty_message_and_remediation(err)


def test_all_wizard_error_variants_have_message_and_remediation() -> None:
    """Each concrete wizard error variant (RVT-850, RVT-880 through RVT-886) has non-empty message and remediation."""
    # Feature: catalog-creation-wizard, Property 13: All wizard errors have message and remediation
    errors = [
        CLIError(
            code=RVT_850,
            message="No rivet.yaml found in /some/path.",
            remediation="Run 'rivet init' to create a project, or use --project to specify the project directory.",
        ),
        CLIError(
            code=RVT_880,
            message="Profile 'staging' not found.",
            remediation="Available profiles can be found in your profiles file. Use --profile to specify a valid profile.",
        ),
        CLIError(
            code=RVT_881,
            message="Missing required options: host, port",
            remediation="Provide all required options via --option key=value flags.",
        ),
        CLIError(
            code=RVT_882,
            message="No catalog plugins are registered.",
            remediation="Install a catalog plugin package (e.g. rivet-duckdb, rivet-postgres) and retry.",
        ),
        CLIError(
            code=RVT_883,
            message="Failed to write catalog to /path/profiles.yaml: Permission denied",
            remediation="Check file permissions and available disk space.",
        ),
        CLIError(
            code=RVT_884,
            message="Catalog name 'my_pg' already exists in this profile.",
            remediation="Provide a valid catalog name matching [a-z][a-z0-9_]* (max 64 chars).",
        ),
        CLIError(
            code=RVT_885,
            message="Plugin validation failed for 'postgres': invalid host",
            remediation="Check your --option and --credential values.",
        ),
        CLIError(
            code=RVT_886,
            message="Connection test failed for 'my_pg': timeout after 30s",
            remediation="Check your connection options and try again.",
        ),
    ]
    for err in errors:
        assert err.message, f"Empty message for {err.code}"
        assert err.remediation, f"Empty remediation for {err.code}"
