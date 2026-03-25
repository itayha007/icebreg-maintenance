package org.example.maintenance;

import lombok.RequiredArgsConstructor;
import org.apache.iceberg.Table;
import org.example.maintenance.impl.CompactionMaintainer;
import org.example.maintenance.impl.DeleteOrphanFilesMaintainer;
import org.example.maintenance.impl.ExpireSnapshotsMaintainer;
import org.example.maintenance.impl.RewriteManifestsMaintainer;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class SparkIcebergMaintenanceVisitor implements IcebergMaintenanceVisitor {

    private final CompactionMaintainer compactionMaintainer;
    private final DeleteOrphanFilesMaintainer deleteOrphanFilesMaintainer;
    private final RewriteManifestsMaintainer rewriteManifestsMaintainer;
    private final ExpireSnapshotsMaintainer expireSnapshotsMaintainer;

    @Override
    public void visitCompaction(Table table) {
        compactionMaintainer.maintain(table);
    }

    @Override
    public void visitDeleteOrphanFiles(Table table) {
        deleteOrphanFilesMaintainer.maintain(table);
    }

    @Override
    public void visitRewriteManifests(Table table) {
        rewriteManifestsMaintainer.maintain(table);
    }

    @Override
    public void visitExpireSnapshots(Table table) {
        expireSnapshotsMaintainer.maintain(table);
    }
}
