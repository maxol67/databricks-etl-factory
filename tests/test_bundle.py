"""Regression check: the bundle must still validate after any resources/ or src/ change.

Catches YAML/resource-reference breakage (a broken `pipeline_id` reference, a stale path after
a rename, ...) - exactly the class of mistake this project's own history has hit and hand-caught
repeatedly while restructuring resources/jobs. Skips (doesn't fail) when the CLI or a working
profile isn't available, so a clone without Databricks credentials configured doesn't get a
false failure - it only fails on a genuine validation error.
"""

import os
import shutil
import subprocess

import pytest

PROFILE = os.environ.get("DATABRICKS_CLI_PROFILE", "free")


def test_bundle_validates_dev():
    if shutil.which("databricks") is None:
        pytest.skip("databricks CLI not installed")

    result = subprocess.run(
        ["databricks", "bundle", "validate", "--profile", PROFILE, "-t", "dev"],
        capture_output=True,
        text=True,
        check=False,
    )

    if result.returncode != 0 and any(s in result.stderr.lower() for s in ("token", "credential", "oauth", "auth")):
        pytest.skip(f"no valid auth for profile {PROFILE!r}: {result.stderr.strip()}")

    assert result.returncode == 0, result.stderr
