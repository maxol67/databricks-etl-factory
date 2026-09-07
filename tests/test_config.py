"""Config integrity check: config/sources.yml and config/source_environment.yml must stay
consistent with each other and with the actual Bronze pipeline code, the same way a foreign key
would enforce it in a database - see CONVENTIONS.md's "Source registry and source x environment
connection config" section for why this is a check script instead. Run this after any change to
either config file or to a Bronze transformation file's SOURCE_NAME.

No `spark` fixture needed - this only reads local YAML/Python files, same as test_bundle.py.
"""

import re
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).parent.parent
REQUIRED_ENVIRONMENTS = {"dev", "prod"}


def _source_names() -> set[str]:
    sources = yaml.safe_load((REPO_ROOT / "config/sources.yml").read_text())["sources"]
    names = [s["name"] for s in sources]
    assert len(names) == len(set(names)), f"duplicate name(s) in config/sources.yml: {names}"
    return set(names)


def _source_environment() -> dict:
    return yaml.safe_load((REPO_ROOT / "config/source_environment.yml").read_text())


def _source_names_in_code() -> set[str]:
    names = set()
    for path in REPO_ROOT.glob("src/pipelines/bronze/*/transformations/*.py"):
        match = re.search(r'^SOURCE_NAME = "(\w+)"', path.read_text(), re.MULTILINE)
        if match:
            names.add(match.group(1))
    return names


def test_every_source_environment_entry_has_a_registered_source():
    sources = _source_names()
    source_environment = _source_environment()

    orphans = set(source_environment) - sources
    assert not orphans, f"config/source_environment.yml names unregistered source(s): {orphans}"


def test_every_registered_source_has_a_source_environment_entry():
    sources = _source_names()
    source_environment = _source_environment()

    missing = sources - set(source_environment)
    assert not missing, f"config/sources.yml source(s) missing from config/source_environment.yml: {missing}"


def test_every_source_environment_entry_covers_dev_and_prod():
    for source, environments in _source_environment().items():
        missing = REQUIRED_ENVIRONMENTS - set(environments)
        assert not missing, f"{source!r} in config/source_environment.yml is missing environment(s): {missing}"


def test_every_source_environment_field_set_has_exactly_one_landing_path():
    for source, environments in _source_environment().items():
        for env_name, fields in environments.items():
            assert "schema_location" in fields, f"{source!r}/{env_name!r} is missing schema_location"
            landing_path_keys = {"staging_path", "drop_path"} & fields.keys()
            assert len(landing_path_keys) == 1, (
                f"{source!r}/{env_name!r} must have exactly one of staging_path/drop_path, "
                f"found: {landing_path_keys or 'neither'}"
            )


def test_every_source_has_a_non_empty_type_and_description():
    sources = yaml.safe_load((REPO_ROOT / "config/sources.yml").read_text())["sources"]
    for source in sources:
        assert source.get("type"), f"{source.get('name')!r} in config/sources.yml has no type"
        assert source.get("description"), f"{source.get('name')!r} in config/sources.yml has no description"


def test_every_bronze_pipeline_source_name_is_registered():
    sources = _source_names()
    in_code = _source_names_in_code()

    unregistered = in_code - sources
    assert not unregistered, f"Bronze SOURCE_NAME(s) not registered in config/sources.yml: {unregistered}"

    orphaned = sources - in_code
    assert not orphaned, f"config/sources.yml source(s) with no matching Bronze pipeline: {orphaned}"
