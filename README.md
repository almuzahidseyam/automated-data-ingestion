# 📊 Automated Data Ingestion Pipeline

![Python](https://img.shields.io/badge/Python-3.11-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Containerized-2496ED?style=for-the-badge&logo=docker&logoColor=white)
![GitHub Actions](https://img.shields.io/badge/CI%2FCD-Automated-2088FF?style=for-the-badge&logo=github-actions&logoColor=white)
![Status](https://img.shields.io/badge/Status-Active-brightgreen?style=for-the-badge)

An enterprise-grade, continuous data ingestion microservice. This repository aggregates highly distributed system metrics, validates the anomalies via a heuristic engine, and auto-generates aggregated logs through continuous automated Pull Requests.

## 🚀 Architecture Overview

This project is designed to simulate and ingest massive continuous data streams from remote edge nodes. The data pipeline is fully autonomous:

1. **Edge Collection**: Remote nodes execute system state dumps.
2. **Sanitization**: The Python daemon (ingest.py) parses and sanitizes the telemetry data.
3. **Automated Version Control**: The service dynamically creates unique branches and Pull Requests to safely merge the immutable metric logs into the main production branch.

`mermaid
graph TD;
    A[Edge Nodes] -->|Raw Metrics| B(ingest.py)
    B -->|Sanitized Data| C{Validation Engine}
    C -->|Approved| D[Automated Git Branch]
    D -->|GitHub API| E[Pull Request]
    E -->|Auto-Merge| F[(Main Branch Archive)]
`

## ⚙️ How it Works

The autonomous bot generates thousands of PRs seamlessly to ensure each log iteration is isolated, tested (if CI hooks are enabled), and safely merged without merge conflicts. This guarantees that system_logs.csv remains a high-integrity chronological ledger of system states.

#Tested by Muhammad
