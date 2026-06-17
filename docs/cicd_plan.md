# CI/CD Plan

## Purpose
The CI/CD setup supports structured development, validation, and promotion of the Databricks Data Governance project.

## Branch Strategy
- main: stable production branch
- dev: integration and testing branch
- ci-cd: CI/CD configuration branch
- role-specific branches: used by team members for individual tasks

## Current Validation
Because the project uses Databricks Community Edition, full automated deployment using Databricks CLI and Databricks Asset Bundles is not executed.

Instead, GitHub Actions is used to validate the repository structure and confirm that the required pipeline and validation files exist.

## Validated Components
- Silver transformation logic
- Gold transformation logic
- Silver validation notebook
- Gold validation notebook
- Databricks bundle configuration file

## CI/CD Flow
1. Team members work in role-specific branches.
2. Completed work is merged into dev.
3. The ci-cd branch receives the latest changes from dev.
4. GitHub Actions validates the project structure.
5. Notebooks are tested manually in Databricks Community Edition.
6. After successful testing, dev is promoted to main.

## Limitation
Since Databricks Community Edition is used, real automated deployment to DEV and PROD environments is documented but not fully executed.
