#!/bin/bash
# deploy.sh
# Validates and deploys the etl_factory bundle.
# Usage: ./deploy.sh [--target dev] [--profile free]

PROFILE="free"
TARGET="dev"

# Pull --target/--profile out of the CLI args.
while [[ "$#" -gt 0 ]]; do
    case $1 in
        --target) TARGET="$2"; shift ;;
        --profile) PROFILE="$2"; shift ;;
    esac
    shift
done

echo "Validating bundle (target: $TARGET)..."
databricks bundle validate --target "$TARGET" --profile "$PROFILE"

echo "Deploying bundle..."
databricks bundle deploy --target "$TARGET" --profile "$PROFILE"

echo "Done - deployed to target '$TARGET'."
