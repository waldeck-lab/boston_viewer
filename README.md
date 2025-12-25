# Boston Viewer

[![Python](https://img.shields.io/badge/python-3.10%2B-blue.svg)](#requirements)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](#license)
[![Platform](https://img.shields.io/badge/platform-linux%20%7C%20wsl-lightgrey.svg)](#requirements)
[![Status](https://img.shields.io/badge/status-active-success.svg)](#)

Local, reproducible data pipeline and SQLite database for all accepted Swedish
**Lepidoptera (fjärilar)** using Dyntaxa / Artdatabanken APIs.

---

## Overview

This project builds and maintains a **local authoritative snapshot** of all
accepted Swedish Lepidoptera species. It is designed for research and analysis
use cases where stable identifiers, reproducibility, and low upstream API load
are important.

Key features:

- Resolves Lepidoptera taxonomy via Dyntaxa
- Local JSON cache to minimize API traffic
- Batch-based refresh using POST `/taxa`
- SQLite database with stable local indices
- Incremental consolidation (insert / update / reactivate / deactivate)
- Full change history per refresh run

---

## Requirements

- Linux or WSL2
- Python **3.10+**
- Git
- Dyntaxa / Artdatabanken API subscription key

The API key must be provided via environment variable:

```bash
export ARTDB_KEY=...
