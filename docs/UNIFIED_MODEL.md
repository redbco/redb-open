## Unified Model Overview

Unified Model is reDB’s technology-agnostic schema layer that normalizes database structures across paradigms (relational, document, graph, vector, search, key-value, columnar, wide-column, object storage). It powers schema understanding, comparison, conversion, analytics, and privacy-aware detection.

### Why it matters

- One schema to rule them all: consistent representation across heterogeneous databases
- Safer changes: structured diffing, similarity, and migration complexity scoring
- Portability: metadata-driven type conversion and cross-paradigm translation
- Privacy-by-design: detection surfaces without persisting sensitive data
- AI-ready: clear, typed context that agents can reason about

### Core building blocks

- Schema model (pkg): rich types for tables, collections, nodes, views, indexes, constraints, functions, users/roles, and more
- Metrics and analytics (pkg): size/perf/quality metrics decoupled from structure
- Enrichment (pkg): AI-derived classification, compliance summaries, recommendations
- Conversion framework (pkg): metadata-driven type conversions and strategies
- Comparison (pkg): structural diffs, similarity, and migration complexity

See `pkg/unifiedmodel/README.md` for the full API surface. This document focuses on the big picture and the service.

## Unified Model Service (services/unifiedmodel)

Microservice that operationalizes the unified model capabilities behind a gRPC API and integrates with the supervisor/runtime.

### Responsibilities

- Expose gRPC endpoints for schema versioning, comparison, metrics, enrichment, and conversion
- Orchestrate detection pipelines (schema-only, enriched, sample-data assisted)
- Provide consistency and policy checks before persisting or acting on schemas
- Serve analytics and summaries for UIs, CLIs, and automations
- Integrate with supervisor for lifecycle, health, and metrics

### Architecture

- Base service lifecycle: init → start → health → graceful stop
- Shared gRPC server registration: registers `UnifiedModelService` on startup
- Engine modules:
  - translator: same- and cross-paradigm translation with capability filtering
  - engine: conversion orchestration and server integration
  - comparison: schema model diffing and similarity scoring
  - matching: unified object matching utilities
  - detection: detection interfaces and levels
  - classifier: feature extraction, scoring, and ingest adapters
  - generators: test/data model generators (e.g., postgres, mysql, mongodb, cassandra, neo4j, edgedb)

Service entrypoint: `services/unifiedmodel/cmd/main.go`

### Capabilities exposed (conceptual)

- Schema versioning and retrieval
- Structural comparison with similarity and breaking-change signals
- Cross-paradigm translation and metadata-driven type conversion
- Metrics and analytics summaries
- Detection orchestration across levels (schema-only → enriched → sample-assisted)

Note: concrete RPCs are defined in `api/proto/unifiedmodel/v1/unifiedmodel.proto` and implemented in the engine/server.

### Health, metrics, and ops

- Health checks: gRPC server live, engine status
- Metrics: processed requests, errors (extensible)
- Hot reload support via supervisor restart keys for config changes

## Benefits summary

- Technology-agnostic schemas enable consistent tooling and automation
- Safer changes through structured diffs and migration guidance
- Scalable conversion via metadata instead of hardcoded rules
- Privacy-first detection without persisting sensitive samples
- Clear service boundaries with typed gRPC contracts

## Where to start

- Service: `services/unifiedmodel/` (engine, translator, detectors, generators)
- Types and utilities: `pkg/unifiedmodel/`
- Protos: `api/proto/unifiedmodel/v1/unifiedmodel.proto`

