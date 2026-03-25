# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Run

```bash
# Build (skip tests)
mvn clean package -DskipTests

# Run
mvn spring-boot:run

# Run with a specific Spring profile
mvn spring-boot:run -Dspring-boot.run.profiles=<profile>
```

## Architecture

This is a Spring Boot + Apache Spark application that runs Iceberg table maintenance operations against configured databases.

### Entry point
`IcebregMaintenanceApplication` implements `CommandLineRunner` — on startup it calls `MaintenanceService.runMaintenance()` and exits.

### Maintenance pipeline
1. `MaintenanceService` reads the list of databases from config, calls `icebergCatalog.listTables()` for each, then runs each enabled maintenance type against every table in parallel (`parallelStream`).
2. `MaintenanceType` enum is the visitor dispatcher — each constant overrides the single abstract method `void visit(IcebergMaintenanceVisitor visitor, Table table)` to call the matching method on the visitor.
3. `SparkIcebergMaintenanceVisitor` is the single Spring `@Component` implementing `IcebergMaintenanceVisitor`. It delegates each visit method to a dedicated `@Component` class in `maintenance/impl/`.
4. The four impl classes (`CompactionMaintainer`, `DeleteOrphanFilesMaintainer`, `RewriteManifestsMaintainer`, `ExpireSnapshotsMaintainer`) each hold a `SparkSession` and execute the corresponding `SparkActions` operation.

### Configuration
All runtime config is in `src/main/resources/application.yml` under two prefixes:
- `iceberg.*` — bound to `IcebergProperties` (`@ConfigurationProperties`). Key fields: `databases` (list), `catalog.name/type/warehouse`, `catalog.glue.*`, `maintenance.enabled-types`.
- `spark.*` — read via `@Value` in `SparkConfig`.

`SparkConfig` builds the `SparkSession` bean with Iceberg extensions and catalog config derived from `IcebergProperties`. `IcebergCatalogConfig` builds the raw Iceberg `Catalog` bean (supports `glue`, `rest`, `hive`, `hadoop`).

### Key dependency versions
- Spring Boot: 2.7.3
- Apache Spark: 3.5.1 (Scala 2.12)
- Apache Iceberg: 1.7.1 (`iceberg-spark-runtime-3.5_2.12`, `iceberg-aws-bundle`)
- Hadoop: 3.4.0
- Java: 11
