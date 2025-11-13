# CAAS IEP – Databricks Asset Bundles (DAB)

This repository contains the Databricks Asset Bundles (DAB) framework used for the CAAS IEP project.  
It defines all data pipelines, tasks, notebooks, reusable YAML fragments, and the shared Python wheel that powers the ingestion and transformation layers across Bronze → Silver → Gold.

---

## 📁 Repository Structure

bundles/ # DAB bundle definitions for dev/stg/prod
common/ # Shared Python utilities and config fragments
fragments/ # Reusable YAML snippets (clusters, secrets, catalogs)
jobs/notebooks/ # All Databricks notebooks used by pipelines
tasks/ # Job/task definitions referenced by bundles
tests/ # Unit tests for merge logic, utils, parsing
tools/ # Helper scripts (validation, deployment, packaging)
wheel/ # Python wheel source (shared business logic)
Makefile # Automation commands (build, deploy, validate)

yaml
Copy code

---

## 🚀 What This Project Delivers

### **Unified Pipeline Deployment**
All ingestion jobs (FR24, AMHS FPL/CNL, ADS-B, hourly Bronze/Silver updates) are defined and deployed through Databricks Asset Bundles.

### **Standardized Configuration**
`fragments/` provides modular YAML components for:
- cluster definitions  
- UC catalog/schema references  
- secret scopes & AWS creds  
- shared pipeline parameters  

### **Shared Python Wheel**
The `wheel/` package contains core business logic:
- FPL/CNL merge rules  
- DOF extraction  
- taxi-in/out calculations  
- Solace exponential backoff  
- parsing + validation utilities  

Built via the Makefile and shipped with every deployment.

### **Automated Testing**
`tests/` includes coverage for:
- merge correctness  
- transformation consistency  
- row-drift prevention  

---

## 🧩 Deployment (DAB)

Validate:
```sh
databricks bundle validate
Deploy:

sh
Copy code
databricks bundle deploy --target dev
Run pipelines:

sh
Copy code
databricks bundle run fr24_bronze_to_silver
🔨 Makefile Commands
sh
Copy code
make build        # Build wheel + lint + format
make validate     # Validate DAB config
make deploy-dev   # Deploy to dev
make deploy-stg   # Deploy to staging
make deploy-prod  # Deploy to production
📦 Key Features
Bronze → Silver ingestion with incremental "build-missing" logic

AMHS FPL/CNL merging with full cancellation rules

DOF extraction from other or EOBT fallback

Real-time ADS-B taxi time calculations

Solace reconnection with exponential backoff

Consistent schema validation across pipelines

🧭 Requirements
Databricks CLI v0.270+

Python 3.10+

AWS (S3, MSK, Secrets Manager) access

Unity Catalog-enabled workspace
