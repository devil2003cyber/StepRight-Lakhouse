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
and GitHub Actions. No local Spark or local Databricks development environment
is required.

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

## Development workflow

All development is performed in Databricks. The repository is used for source
control, bundle definitions and CI/CD:

1. Develop and test notebooks, pipelines and jobs in Databricks.
2. Store the implementation in the GitHub repository.
3. Run unit tests through GitHub Actions.
4. Validate and deploy the bundle to UAT.
5. Run UAT integration tests against the deployed environment.
6. Trigger production deployment manually after approval.
7. Run production smoke tests without modifying production data.

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

## Resume summary

Built an end-to-end retail lakehouse on Databricks using Unity Catalog,
Volumes, Lakeflow Declarative Pipelines, PySpark and Databricks Workflows.
Implemented CDC and batch ingestion, Medallion-layer transformations, data
quality monitoring, Gold analytics datasets and Lakeview dashboards. Automated
UAT and production deployments with Databricks Asset Bundles and GitHub
Actions, including integration testing and read-only production smoke tests.
