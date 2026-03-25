package org.example.maintenance;

import org.apache.iceberg.Table;

public enum MaintenanceType {

    COMPACTION {
        @Override
        public void visit(IcebergMaintenanceVisitor visitor, Table table) {
            visitor.visitCompaction(table);
        }
    },

    DELETE_ORPHAN_FILES {
        @Override
        public void visit(IcebergMaintenanceVisitor visitor, Table table) {
            visitor.visitDeleteOrphanFiles(table);
        }
    },

    REWRITE_MANIFESTS {
        @Override
        public void visit(IcebergMaintenanceVisitor visitor, Table table) {
            visitor.visitRewriteManifests(table);
        }
    },

    EXPIRE_SNAPSHOTS {
        @Override
        public void visit(IcebergMaintenanceVisitor visitor, Table table) {
            visitor.visitExpireSnapshots(table);
        }
    };

    public abstract void visit(IcebergMaintenanceVisitor visitor, Table table);
}
