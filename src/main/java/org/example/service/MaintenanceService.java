package org.example.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
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
        this.loadTables().parallelStream()
                .forEach(table -> {
                    enabledTypes.forEach(type -> type.getMaintainer(spark).maintain(table));
                });
    }

    private List<Table> loadTables() {
        return properties.getDatabases().stream()
                .flatMap(db -> icebergCatalog.listTables(Namespace.of(db)).stream())
                .map(icebergCatalog::loadTable)
                .collect(Collectors.toList());
    }
}
