# StepRight Lakehouse

## Project overview

StepRight Lakehouse is an end-to-end retail analytics platform built entirely
on Databricks. It ingests batch files and change data capture (CDC) data,
applies data quality checks and transformations, and publishes curated
analytics tables and dashboards for business reporting.

The project follows a Medallion architecture:

```text
Raw sources
    |
    v
Bronze - ingestion and source-aligned data
    |
    v
Silver - cleaned, deduplicated and modeled data
    |
    v
Gold - business-ready analytical datasets
    |
    v
SQL dashboard and daily reporting
```

Development, testing, deployment and orchestration are performed in Databricks
and GitHub Actions. The standalone data generator can run locally, but no
local Spark or local Databricks development environment is required.

## Business use case

The platform supports retail and e-commerce analytics across:

- Customers and customer change events
- Orders and order items
- Products and categories
- Inventory
- Clickstream activity
- Fulfillment performance
- Revenue and conversion funnel reporting

The final Gold layer provides datasets for:

- Daily revenue analysis
- Product performance
- Customer 360 analysis
- Conversion funnel analysis
- Fulfillment health monitoring

## Architecture

### Data ingestion

The ingestion pipeline processes seven source domains:

- `orders_cdc`
- `order_items_cdc`
- `customers_cdc`
- `products`
- `categories`
- `clickstream`
- `inventory`

Source data is stored in a Unity Catalog Volume landing zone. The ingestion
pipeline uses Databricks file ingestion and streaming patterns to load data
into the Bronze layer.

CDC sources use a Debezium-style envelope containing `op`, `ts_ms`, `before`,
and `after`. CSV and JSON file sources use explicit schemas rather than schema
inference. Bronze preserves the source structure and adds `_ingested_at` and
`_source_file` technical metadata for lineage and troubleshooting.

### Transformation

The transformation pipeline applies:

- Schema and data type standardization
- Deduplication
- CDC handling
- Data cleansing
- Business joins
- Derived metrics
- Gold-layer aggregations

The pipeline is separated into Silver and Gold transformations so that
source-aligned data and business-facing data remain independently manageable.

Silver reads only published Bronze-valid tables. Orders and customers use
SCD Type 2 because status and profile history are analytically meaningful.
Products, categories, and order items use SCD Type 1 where current state is
sufficient. Gold materialized views provide business-facing datasets for
revenue, product performance, Customer 360, conversion funnel, and fulfillment
health.

### Data quality

Data quality is checked during orchestration and pipeline execution. The
project includes:

- Pipeline expectations
- Data quality event processing
- Failure-rate monitoring
- DQ dashboard setup
- A job-level DQ validation step
- Smoke and integration tests

Quality failures are surfaced through pipeline event logs and dashboard
monitoring rather than being silently ignored.

The quality design has separate responses for different failure types:

- Row-level expectations identify structurally or semantically invalid rows.
- Valid and quarantined tables preserve both outcomes without silent data loss.
- A job-level DQ gate blocks transformation when a run has no current Bronze
  data or an excessive quarantine rate.
- Event hooks alert when an individual expectation exceeds its failure-rate
  threshold.
- Anti-join checks monitor referential integrity across Silver tables.

### Orchestration

The orchestration job runs the complete processing sequence:

```text
Ingestion pipeline
        |
        v
DQ validation
        |
        v
Transformation pipeline
        |
        v
Daily report generation
```

The environment bootstrap job creates the catalog, schema, landing Volume and
source folders, then runs the ingestion, transformation, event-log and
dashboard setup tasks.

The recurring orchestration job is intentionally separate from bootstrap:

```text
run_ingestion
      |
      v
dq_check
      |
      v
run_transformation
      |
      v
report
```

The repository defines this dependency graph but does not currently define a
time-based Databricks schedule. The job can be triggered manually, through the
Databricks CLI/API, or by an external scheduler. A production schedule or
file-arrival trigger should be selected according to the source delivery
contract.

## Technology stack

- **Databricks**
  - Unity Catalog
  - Databricks Volumes
  - Lakeflow Declarative Pipelines
  - Databricks Workflows
  - Databricks SQL Warehouse
  - Lakeview dashboards
- **Apache Spark / PySpark**
  - Batch and streaming data processing
  - DataFrame transformations
  - SQL-based analytical modeling
- **Python**
  - Notebook tasks
  - Spark Python reporting jobs
  - Data quality logic
  - Automated tests
- **Databricks Asset Bundles**
  - Declarative deployment of pipelines, jobs and dashboards
  - Target-specific catalog and warehouse configuration
- **GitHub Actions**
  - Unit testing
  - UAT validation and deployment
  - Integration testing
  - Manually approved production deployment
- **pytest**
  - Ingestion unit tests
  - Transformation unit tests
  - UAT integration tests
  - Production smoke tests

## Environments

Development and UAT use the same Databricks workspace but separate Unity
Catalogs:

```text
Development  -> dev catalog
UAT          -> uat catalog
```

Production uses a separate Databricks workspace and production warehouse:

```text
Production   -> prod catalog
```

Warehouse IDs are workspace-specific. The UAT warehouse belongs to the shared
development/UAT workspace, while the production warehouse belongs to the
production workspace.

Deploying the bundle creates the production platform but does not copy dev or
UAT data into production. Workspaces have separate Volumes, catalogs,
pipelines, checkpoints, credentials, and data. Production must receive its
own source delivery. The recommended initial-load sequence is:

```text
Deploy production bundle
        |
        v
Create production landing/staging infrastructure
        |
        v
Generate or deliver batch_0
        |
        v
Upload batch_0 to production staging
        |
        v
Copy batch_0 to production landing
        |
        v
Full refresh ingestion, then full refresh transformation
```

After the initial load, generate or receive incremental batches, copy them to
the production landing Volume, and run the orchestration job. The loader
should be catalog-parameterized before it is used for production; the current
course loader contains development-specific paths.

## Development workflow

Pipeline development is performed in Databricks, while the source repository
contains the implementation, bundle definitions, tests, and CI/CD workflows:

1. Generate or receive source batches.
2. Develop and test notebooks, pipelines and jobs in Databricks.
3. Store the implementation in the GitHub repository.
4. Run ingestion and transformation unit tests.
5. Validate and deploy the bundle to UAT.
6. Run UAT integration tests against the deployed environment.
7. Trigger production deployment manually after approval.
8. Run production smoke tests without modifying production data.

## Deployment

The Databricks Asset Bundle is defined in
[bundle/databricks.yml](./bundle/databricks.yml). It deploys:

- Ingestion pipeline
- Transformation pipeline
- Orchestration job
- Environment bootstrap job
- Data quality dashboard

The UAT workflow is defined in
[.github/workflows/ci.yml](./.github/workflows/ci.yml).

The production workflow is defined in
[.github/workflows/deploy-prod.yml](./.github/workflows/deploy-prod.yml).
Production deployment requires the exact confirmation phrase
`DEPLOY PROD`.

Bundle variables are passed explicitly for each environment:

```bash
databricks bundle validate --target uat \
  --var="stepright_warehouse_id=${DATABRICKS_WAREHOUSE_ID_UAT}"

databricks bundle deploy --target uat \
  --var="stepright_warehouse_id=${DATABRICKS_WAREHOUSE_ID_UAT}"
```

The UAT workflow runs unit tests, ensures the UAT catalog exists, validates
and deploys the bundle, runs environment bootstrap, and then runs integration
tests. The production workflow is independently and manually triggered,
requires the exact confirmation phrase `DEPLOY PROD`, deploys with separate
production credentials, and finishes with read-only smoke tests.

The project uses source synchronization through Databricks Asset Bundles; it
does not currently build a Python wheel or Docker image. A wheel would be a
reasonable future option for a larger shared Python library.

## Security and secrets

Credentials are not stored in source code.

GitHub Actions uses repository secrets for deployment:

```text
DATABRICKS_HOST_UAT
DATABRICKS_TOKEN_UAT
DATABRICKS_WAREHOUSE_ID_UAT

DATABRICKS_HOST_PROD
DATABRICKS_TOKEN_PROD
DATABRICKS_WAREHOUSE_ID_PROD
```

Credentials required by notebook or pipeline runtime code, such as external
API tokens, database passwords or alerting webhooks, should be stored in a
Databricks Secret Scope and accessed with `dbutils.secrets.get()`.

Development and production credentials should use separate tokens or service
principals with the minimum required permissions.

## Testing

The project includes:

- Ingestion pipeline unit tests
- Transformation pipeline unit tests
- UAT integration tests
- Production read-only smoke tests

Production smoke tests verify that:

- Pipelines are deployed and reachable
- The orchestration job is reachable
- The dashboard is reachable
- The latest pipeline update did not fail
- Gold tables can be queried through the production SQL warehouse

Row counts in production are informational and are not used as hard-coded
assertions.

The test strategy is layered:

```text
Unit tests
    |
    v
UAT integration tests
    |
    v
Production smoke tests
```

Unit tests run with pytest and local Spark where DataFrame behavior is
required. They use small, hand-crafted inputs to test SCD2 filtering,
discount allocation, inventory snapshot selection, anonymous-session funnel
behavior, null handling, quarantine-rule generation, and SLA boundaries.

Integration tests use a fixed seed, fixed as-of date, and a small generated
batch. They upload data to UAT, trigger the deployed resources, and verify
end-to-end behavior. They also use row conservation:

```text
valid rows + quarantined rows = expected source rows
```

Production smoke tests are read-only. They verify resource reachability,
pipeline health, dashboard availability, and Gold-table queryability without
asserting dynamic production row counts.

## Data generator and local batch loading

[data_generator.py](./data_generator.py) simulates the external retail source
systems. Batch 0 is an initial snapshot; batch 1 and later contain incremental
customers, orders, order items, clickstream events, inventory snapshots, and
selected updates. Categories are intentionally unchanged in ordinary
incremental batches.

The generator stores IDs and relationships in a state file so new records can
reference earlier batches safely. For reproducible tests, provide both
`--seed` and `--as-of`:

```bash
python data_generator.py --batch 0 \
  --seed 42 \
  --as-of 2026-01-01 \
  --output-dir ./it_batches \
  --state-file ./it_batches/generator_state.json \
  --n-customers 20 \
  --n-orders 50 \
  --n-products 15 \
  --n-clickstream 200 \
  --n-inventory-days 2
```

For development or UAT, generated files can be uploaded to the staging
Volume and copied to the landing Volume with
[setup/02_data_loader.py](./setup/02_data_loader.py). The loader is a
course/demo delivery mechanism; production should use an automated source
delivery process such as cloud object storage, a CDC connector, or an
event-driven file-arrival workflow.

## Monitoring views and dashboard

The monitoring setup creates:

- `dq_expectations_history` — expectation pass/fail counts and failure rates
  from ingestion and transformation event logs.
- `dq_quarantine_counts` — current quarantine counts across Bronze and Silver.
- `dq_referential_integrity` — orphan counts for monitored relationships.

The Lakeview dashboard contains Expectations, Quarantine, and Orphans pages.
Expectation history is sourced from pipeline event logs. Quarantine counts are
currently a query-time snapshot of accumulated quarantined rows rather than a
separate persisted daily history table.

The event-hook alert sender currently logs alert messages. A production
notification integration should replace the stub with a Slack, PagerDuty, or
other incident-management webhook retrieved from a Databricks Secret Scope.

## Operational considerations

- Dev and UAT share a workspace but use separate Unity Catalog catalogs.
- Production uses a separate workspace, catalog, warehouse, and credentials.
- The current data loader contains development-specific `dev` paths and should
  accept a catalog parameter before production use.
- Incremental batches may legitimately omit unchanged sources. The DQ gate
  should use a batch manifest or source-specific freshness policy rather than
  requiring every source to produce rows on every run.
- Full refresh should be used for an initial environment load or deliberate
  rebuild; normal incremental runs should use the standard orchestration job.
- Alert thresholds and job-blocking thresholds serve different purposes:
  alerts provide early warning, while the DQ gate acts as a circuit breaker
  for catastrophically unhealthy batches.

## Repository structure

```text
bundle/
  databricks.yml
  resources/
    ingestion_pipeline.yml
    transformation_pipeline.yml
    orchestration_job.yml
    environment_bootstrap_job.yml
    dq_dashboard.yml

pipelines/
  ingestion_pipeline/
    transformations/
    utilities/
    tests/
  transformation_pipeline/
    transformations/
    utilities/
    tests/

jobs/
  dq_check.py
  report.py

monitoring/
  dq_dashboard.lvdash.json
  dq_dashboard_setup.py

setup/
  01_environment_setup.py
  02_data_loader.py
  03_event_log_setup.py

tests/
  integration/
  smoke/
```

## Pipeline screenshots

Add screenshots here to show the project during an interview or portfolio
review. Recommended screenshots:

### 1. Overall Databricks pipeline graph

![description](https://github.com/devil2003cyber/StepRight-Lakhouse/blob/main/docs/images/overall-pipeline-graph.png)

### 2. Ingestion pipeline

<!-- Add image: docs/images/ingestion-pipeline.png -->

![description](https://github.com/devil2003cyber/StepRight-Lakhouse/blob/main/docs/images/ingestion-pipeline.png)

### 3. Transformation pipeline

<!-- Add image: docs/images/transformation-pipeline.png -->

![description](https://github.com/devil2003cyber/StepRight-Lakhouse/blob/main/docs/images/transformation-pipeline.png)

### 4. Data quality dashboard

<!-- Add image: docs/images/data-quality-dashboard.png -->

![description](https://github.com/devil2003cyber/StepRight-Lakhouse/blob/main/docs/images/data-quality-dashboard.png)

### 5. GitHub Actions deployment

<!-- Add image: docs/images/github-actions-deployment.png -->

![description](https://github.com/devil2003cyber/StepRight-Lakhouse/blob/main/docs/images/github-actions-deployment.png)

## summary

Built an end-to-end retail lakehouse on Databricks using Unity Catalog,
Volumes, Lakeflow Declarative Pipelines, PySpark and Databricks Workflows.
Implemented CDC and batch ingestion, Medallion-layer transformations, data
quality monitoring, Gold analytics datasets and Lakeview dashboards. Automated
UAT and production deployments with Databricks Asset Bundles and GitHub
Actions, including integration testing and read-only production smoke tests.
