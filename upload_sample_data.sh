#!/bin/bash
# upload_sample_data.sh
# Uploads this repo's committed sample_data/ into a workspace's Staging/Drop volumes, so the
# six example Bronze pipelines (bronze_sap_etl, bronze_webshop_etl, bronze_crm_etl,
# bronze_finance_etl, bronze_mdm_etl, bronze_prodman_etl) have something to actually ingest.
# Re-run any time to reset back to the committed sample data (--overwrite). Creates each
# destination folder first (databricks fs cp doesn't create parent directories on its own) -
# safe to re-run against volumes that already have them.
# Usage: ./upload_sample_data.sh --catalog <catalog> [--profile free]

PROFILE="free"
CATALOG=""

# Pull --catalog/--profile out of the CLI args.
while [[ "$#" -gt 0 ]]; do
    case $1 in
        --catalog) CATALOG="$2"; shift ;;
        --profile) PROFILE="$2"; shift ;;
    esac
    shift
done

if [ -z "$CATALOG" ]; then
    echo "Usage: $0 --catalog <catalog> [--profile <profile>]" >&2
    echo "Example: $0 --catalog etl_factory_dev --profile free" >&2
    exit 1
fi

upload() {
    local local_file="$1" remote_path="$2" remote_dir
    remote_dir="$(dirname "$remote_path")"
    echo "Uploading $local_file -> $remote_path"
    databricks fs mkdir "dbfs:$remote_dir" --profile "$PROFILE"
    databricks fs cp "$local_file" "dbfs:$remote_path" --profile "$PROFILE" --overwrite
}

upload sample_data/sap/orders/orders.json \
    "/Volumes/$CATALOG/staging/staging/sap/orders/orders.json"
upload sample_data/webshop/orders/orders.json \
    "/Volumes/$CATALOG/drop/system_drop/webshop/orders/orders.json"
upload sample_data/crm/customers/customers.json \
    "/Volumes/$CATALOG/drop/system_drop/crm/customers/customers.json"
upload sample_data/finance/customer_credit_score/customer_credit_score.json \
    "/Volumes/$CATALOG/drop/internal_drop/finance/customer_credit_score/customer_credit_score.json"
upload sample_data/mdm/products/products.json \
    "/Volumes/$CATALOG/staging/staging/mdm/products/products.json"
upload sample_data/prodman/product_pricing/product_pricing.json \
    "/Volumes/$CATALOG/drop/internal_drop/prodman/product_pricing/product_pricing.json"

echo "Done - sample data uploaded to catalog '$CATALOG'."
