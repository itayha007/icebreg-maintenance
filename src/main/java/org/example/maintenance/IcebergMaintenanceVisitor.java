package org.example.maintenance;

import org.apache.iceberg.Table;

public interface IcebergMaintenanceVisitor {

    void visitCompaction(Table table);

    void visitDeleteOrphanFiles(Table table);

    void visitRewriteManifests(Table table);

    void visitExpireSnapshots(Table table);
}
