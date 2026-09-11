# Backlog

Deferred ideas for this project - not scheduled, no priority ranking. When an entry is actually
implemented, fold it into the relevant code/docs and delete it from here rather than marking it
done in place.

## Ingestion / Staging

### Customer Drop zone

Beyond Staging (fed by an external cloud-native or on-prem ETL/orchestration tool, for sources
with no native Lakeflow Connect connector), the target architecture has one more pre-Bronze zone
for non-automated arrivals, split by *who* writes (System Drop and Internal Drop are both
implemented now - see below):

- **Customer Drop** - external customers submitting their own files. Not Databricks-native - even
  an *embedded* Databricks App requires the viewer to already be an authenticated Databricks
  workspace user, so a genuinely external customer needs an external front door (a web form, a
  serverless function, a workflow-automation service - mechanism not yet chosen, hyperscaler
  choice deliberately left open) writing into a **per-customer** storage location, for isolation
  and independent credential revocation. The `drop.customer_drop` volume exists but is unused so
  far.

Idea: implement this alongside the config-driven, agent-generated pipeline layer. Zone *names and definitions*
describe purpose/trust boundary only (who writes there and why) - the storage implementation (a
Unity Catalog Volume vs. a plain cloud object storage location reached via existing tooling) is a
separate, per-engagement choice, not baked into the definition.

Open question, not yet decided: whether Customer Drop content flows into Bronze automatically on
the same trigger as Staging, or needs a review/approval gate first (e.g. a separate Bronze
sub-schema, or a human-triggered run) before being trusted alongside production data - it has no
schema contract at all, unlike System/Internal Drop below.

**System Drop - implemented** for two sources: `bronze_crm_etl`'s `crm_customers.py` reads
`drop.system_drop`'s `crm/customers/` folder, and `bronze_webshop_etl`'s `webshop_orders.py`
reads its `webshop/orders/` folder (one subfolder per source system, one further subfolder per
object within it - a future CRM object like contacts/opportunities would get its own sibling
subfolder). Flows into Bronze automatically via Auto Loader, same trigger as Staging - no
review/approval gate, resolving that part of the open question above, at least for these
sources.

**Internal Drop - implemented**, with a definition slightly broader than originally scoped:
`bronze_finance_etl`'s `finance_customer_credit_score.py` and `finance_customer_abc_classification.py` read `drop.internal_drop`'s
`finance/customer_credit_score/` and `finance/customer_abc_classification/` folders, and `bronze_prodman_etl`'s `prodman_product_pricing.py`
reads its `prodman/product_pricing/` folder. Both are systems pushing files programmatically, not
an employee's manual upload (the original "who writes" distinction between Internal and System
Drop) - placed in Internal Drop anyway since the actual boundary that matters here is
*internal-to-the-org system* vs. *genuinely external/third-party system*, not manual-vs-automated.
Worth keeping in mind if a future source makes that distinction matter more concretely (e.g.
different trust/review requirements for manual uploads specifically).

### Staging as an external volume

The Staging volume (`etl_factory_dev.staging.staging`) is currently a *managed* Unity
Catalog volume. Databricks' own best-practices guidance recommends *external* volumes
specifically for ingestion staging locations fed by external systems (registering that system's
actual storage location into UC) - managed is only appropriate here because there's no real
external tool writing to it yet, just this repo's own committed `sample_data/` (see
`upload_sample_data.sh`), not a genuine external system.

Idea: switch to an external volume once there's a real externally-fed (or equivalent) source
landing files into Staging. Premature before then - there's no real external storage location to
register yet.

## Sources

### A real Lakeflow Connect-covered source

All six current sources go through the Staging/Drop → Auto Loader → Bronze path, since none of
them have a native Lakeflow Connect connector (see README's Target architecture). The other
routing branch - a source with a native connector (e.g. SQL Server) landing directly to Bronze as
governed Delta, no external orchestrator or Staging/Drop involved - has no worked example yet.

Idea: add one such source alongside the existing six, to demonstrate both routing branches side
by side rather than describing the connector path only in prose.

## Modeling

### Unknown members for the SCD Type 2 dimensions

`fact_orders`' `customer_key`/`product_key` are attached via a LEFT JOIN against
`dim_customer`/`dim_product` (see `fact_orders.py`), so an order whose natural key doesn't
resolve to any dimension row gets a NULL surrogate key instead of being dropped - confirmed
live: `CUST-00050` (the deliberately-invalid CRM row, rejected into
`silver_data_quality.crm_customers_findings` - see `customers.py`) never enters
`dim_customer`, so its 7 orders in `fact_orders` correctly show `customer_key IS NULL` while
still carrying `customer_id = 'CUST-00050'`.

Idea: give `dim_customer`/`dim_product` a standard Kimball "unknown member" row each - a
reserved surrogate key (e.g. `-1`) paired with a reserved natural key (e.g. `'UNKNOWN'`), not
generated via `GENERATED ALWAYS AS IDENTITY` like every real row - and change `fact_orders.py`'s
join to `coalesce(dim.customer_key, -1)` (same for `product_key`) instead of leaving it NULL
when nothing matches. Standard practice specifically because many BI tools handle NULL foreign
keys poorly (inner joins that should show "Unknown" as its own bucket instead silently drop
the row from a customer- or product-sliced view). Doesn't change the natural-key-recovery
rule at all (see CONVENTIONS.md's Dimensional modeling naming) - `customer_id`/`product_id`
stay on the fact exactly as they do now; this only gives the *surrogate* key side a non-NULL
fallback for join-based tooling.

## Deployment / operations

### CI pipeline

There's no CI pipeline yet - `.pre-commit-config.yaml`'s `ruff-check`/`ruff-format` hooks are
currently the only enforcement mechanism, and only for whoever has actually run
`pre-commit install` locally (see README's Code Formatting section). Nothing runs
`bundle validate`, the pytest suite, or linting automatically on push/PR.

Idea: a GitHub Actions workflow (or equivalent) running `databricks bundle validate`,
`uv run pytest`, and `ruff check`/`ruff format --check` on every PR - matching the
`tests/test_bundle.py`/`tests/pipelines/.../test_util.py` checks that already exist locally,
see CONVENTIONS.md's Testing section.

### `prod` target's `run_as` under `mode: production`

`databricks.yml`'s `prod` target sets `mode: production` and a `permissions:` block, but no
explicit `run_as:`. Databricks' own `mode: production` validation pushes toward an explicit
`run_as` (ideally a service principal) when one isn't already configured - never actually
confirmed live whether `bundle validate -t prod` warns or errors on this, since the `azure`
profile's OAuth token was expired the one time this was checked.

Idea: run `bundle validate -t prod --profile azure` after re-authenticating that profile, and
either add an explicit `run_as` (a service principal, if/when one exists for this project) or
confirm the current owner-based deploy is an accepted, deliberate choice at this project's
scale.

## Multi-tenancy / configuration

### Config-driven pipeline generation

Every pipeline's actual code is currently hand-written (by a person, or by an AI agent prompted
with full context each time) - see README's "Agentic ETL" section for why
that's already the model this project uses, not a generic runtime interpreting metadata at
execution time.

Idea: a lightweight per-source config (connection/location info, target schema) that an agent
reads and uses to generate/maintain a pipeline's code directly, instead of a person supplying
that context by hand every time - still following this repo's house conventions
(`CONVENTIONS.md`), not a generic runtime interpreting the config at execution time.

The storage question for connection info specifically is settled - see CONVENTIONS.md's "Source
registry and source x environment connection config" section: a plain git-tracked file, not a
database. Whether a fuller per-source generation config (target schema, business rules) needs
something heavier than that is still open, but nothing so far has justified it.

### Project/tenant dimension for source connection config

CONVENTIONS.md's "Source registry and source x environment connection config" section covers
`source` x `environment` (implemented, all six sources). Deliberately not implemented: a
`Project`/tenant dimension on top of that for "the same source system, many independent instances"
(e.g. SAP deployed separately per regional subsidiary) - nothing in this reference project needs
it yet, and adding it before there's a real second tenant would mean guessing its shape.

The general principle to hold onto if that changes: extend `config/sources.yml`/
`config/source_environment.yml` themselves (e.g. nest a `project` layer between source and
environment) rather than reaching for a database - add only the piece actually needed, not a full
relational control-table schema up front.

Key distinction worth keeping, if this is ever added: it would solve *connection/deployment*
variation across many tenants of the *same* source type cheaply (config rows, not code),
regardless of whether the pipeline itself is hand-written, agent-generated, or metadata-driven -
it would **not** solve *genuinely different transformation logic* per tenant (e.g. one
subsidiary's SAP instance has an extra custom field the others don't); that case still needs
either per-tenant logic variation (agent-generated) or a metadata-driven engine's conditional
branching.
