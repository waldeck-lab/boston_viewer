# Architecture

This document describes the high-level architecture and design principles of
the **Boston Viewer** project.

---

## Overview

The system maintains a **local, authoritative snapshot** of all accepted Swedish
Lepidoptera species based on Dyntaxa taxonomy data. The architecture is designed
to be:

- Reproducible
- Incremental
- Low-impact on upstream APIs
- Easy to extend with additional metadata

The pipeline is intentionally linear and explicit.

---

## High-level flow

