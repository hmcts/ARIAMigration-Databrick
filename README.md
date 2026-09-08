# ARIAMigration-Databrick

This Project contains code for Both the ARIA Archive and active cases


## Folder Structure at a glance

Use naming conventions as a guide. This list is for a general overview only.

- ARIA_DABS - *Databricks Jobs and Pipelines configuration.*
  - resources
    - pipelines - *Databricks pipelines to perform an action, e.g. process the GOLD layer of a specific state.*
    - workflows - *Orchestration of pipelines or tasks to run in a specified order.*
- AzureFunctions - *Function App source code to be deployed to the Azure Functions instance.*
- ci_cd_templates - *Templates for CI/CD runs to be used by Azure Pipelines.*
- Databricks - *Source code for Notebooks and Python Functions to be deployed to Databricks.*
  - ACTIVE
    - APPEALS - *Source code for bronze, silver and gold layers data transformation.*
      - shared_functions - *Python code, including for each state, with capability for notebooks to pull functions from other states, hence shared.*
        - dq_rules - *Data Quality SQL checks for each state to be used by the notebooks.*
    - MVP - *Source code for publishing and consuming events for external actions relating to document store or CCD.*
    - tests - *Local tests for manually checking Databricks functionality.*
  - ARCHIVE
- HTML_Templates - *HTML Templates to be stored here.*
- ReferenceData - *Reference Data (static) such as CSVs of lookup tables etc. to be stored here.*
- tests - *Unit tests to be run as part of the CI/CD process.*


## Environments

We currently maintain a number of instances on a number of environments for specific purposes, listed below.

| Environment       | Instances                     |
| ----------------- | ----------------------------- |
| Sandbox (sbox)    | 00 (Development), 01 (Unused) |
| Staging (stg)     | 00 (QA), 01 (Development)     |
| Production (prod) | 00 (Live)                     |

*Note: Production 01 does not exist*

## CI/CD

The CI/CD process is run as part of Azure Pipelines on the Azure DevOps platform.

The [pr-pipeline.yml](pr-pipeline.yml) for CI runs on the opening of a Pull Request. This pipeline is currently mainly used to ensure all unit tests pass before a merge.

The [azure-pipelines_sbox_build.yml](azure-pipelines_sbox_build.yml) to be run manually (or automated on a merge into master branch) to deploy both the Databricks and Azure Function changes. Note that this build has an approval mechanism where definied users in the yaml file are needed to approve before the deployment commences.
