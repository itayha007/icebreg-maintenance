package org.example.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.example.config.IcebergProperties;
import org.example.maintenance.IcebergMaintenanceVisitor;
import org.example.maintenance.MaintenanceType;
import org.example.maintenance.SparkIcebergMaintenanceVisitor;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

@Log4j2
@Service
@RequiredArgsConstructor
public class MaintenanceService {

    private final Catalog icebergCatalog;
    private final IcebergProperties properties;
    private final SparkIcebergMaintenanceVisitor visitor;

    public void runMaintenance() {
        List<MaintenanceType> enabledTypes = this.properties.getMaintenance().getEnabledTypes();
        this.loadTables().parallelStream()
                .forEach(table -> enabledTypes.forEach(type -> type.visit(this.visitor, table)));
    }

    private List<Table> loadTables() {
        return this.properties.getDatabases().stream()
                .flatMap(db -> this.icebergCatalog.listTables(Namespace.of(db)).stream())
                .map(this.icebergCatalog::loadTable)
                .collect(Collectors.toList());
    }
}
