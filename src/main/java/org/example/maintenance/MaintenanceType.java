package org.example.maintenance;

import org.apache.spark.sql.SparkSession;
import org.example.maintenance.impl.CompactionMaintainer;
import org.example.maintenance.impl.DeleteOrphanFilesMaintainer;
import org.example.maintenance.impl.ExpireSnapshotsMaintainer;
import org.example.maintenance.impl.RewriteManifestsMaintainer;

public enum MaintenanceType {

    COMPACTION {
        @Override
        public IcebergMaintainer getMaintainer(SparkSession spark) {
            return new CompactionMaintainer(spark);
        }
    },

    DELETE_ORPHAN_FILES {
        @Override
        public IcebergMaintainer getMaintainer(SparkSession spark) {
            return new DeleteOrphanFilesMaintainer(spark);
        }
    },

    REWRITE_MANIFESTS {
        @Override
        public IcebergMaintainer getMaintainer(SparkSession spark) {
            return new RewriteManifestsMaintainer(spark);
        }
    },

    EXPIRE_SNAPSHOTS {
        @Override
        public IcebergMaintainer getMaintainer(SparkSession spark) {
            return new ExpireSnapshotsMaintainer(spark);
        }
    };

    public abstract IcebergMaintainer getMaintainer(SparkSession spark);
}
