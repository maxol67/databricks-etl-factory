#!/bin/bash
# bump_version.sh
# Increments the version in pyproject.toml's [project] version field.
# Usage: ./bump_version.sh [--minor] [--major]
# Default: increments patch version.

# Read current version.
current=$(grep -m1 '^version = ' pyproject.toml | sed -E 's/version = "(.*)"/\1/')
major=$(echo $current | cut -d. -f1)
minor=$(echo $current | cut -d. -f2)
patch=$(echo $current | cut -d. -f3)

# Increment version based on argument.
if [[ "$1" == "--major" ]]; then
    major=$((major + 1))
    minor=0
    patch=0
elif [[ "$1" == "--minor" ]]; then
    minor=$((minor + 1))
    patch=0
else
    patch=$((patch + 1))
fi

new_version="$major.$minor.$patch"

# Write new version.
perl -pi -e "s/^version = \"$current\"/version = \"$new_version\"/" pyproject.toml

echo "Version bumped: $current -> $new_version"
