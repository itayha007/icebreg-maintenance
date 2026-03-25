package org.example.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.spark.sql.SparkSession;
import org.example.config.IcebergProperties;
import org.example.maintenance.MaintenanceType;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

@Log4j2
@Service
@RequiredArgsConstructor
public class MaintenanceService {

    private final SparkSession spark;
    private final Catalog icebergCatalog;
    private final IcebergProperties properties;

    public void runMaintenance() {
        List<MaintenanceType> enabledTypes = properties.getMaintenance().getEnabledTypes();
        List<String> databases = properties.getDatabases();

        log.info("Starting Iceberg maintenance for databases: {}", databases);
        log.info("Enabled maintenance types: {}", enabledTypes);

        for (String database : databases) {
            List<Table> tables = loadTablesFromDatabase(database);
            log.info("Found {} tables in database '{}'", tables.size(), database);

            for (Table table : tables) {
                for (MaintenanceType type : enabledTypes) {
                    try {
                        log.info("Running {} on {}", type, table.name());
                        type.getMaintainer(spark).maintain(table);
                    } catch (Exception e) {
                        log.error("Failed to run {} on table {}: {}", type, table.name(), e.getMessage(), e);
                    }
                }
            }
        }

        log.info("Iceberg maintenance completed.");
    }

    private List<Table> loadTablesFromDatabase(String database) {
        Namespace namespace = Namespace.of(database);
        return icebergCatalog.listTables(namespace).stream()
                .map(identifier -> {
                    try {
                        return icebergCatalog.loadTable(identifier);
                    } catch (Exception e) {
                        log.error("Failed to load table {}: {}", identifier, e.getMessage(), e);
                        return null;
                    }
                })
                .filter(table -> table != null)
                .collect(Collectors.toList());
    }
}
