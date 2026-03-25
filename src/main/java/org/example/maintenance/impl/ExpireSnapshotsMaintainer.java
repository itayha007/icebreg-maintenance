package org.example.maintenance.impl;

import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.iceberg.Table;
import org.apache.iceberg.spark.actions.SparkActions;
import org.apache.spark.sql.SparkSession;
import org.example.maintenance.IcebergMaintainer;

@Log4j2
@RequiredArgsConstructor
public class ExpireSnapshotsMaintainer implements IcebergMaintainer {

    private final SparkSession spark;

    @Override
    public void maintain(Table table) {
        log.info("Expiring snapshots on table: {}", table.name());
        SparkActions.get(spark)
                .expireSnapshots(table)
                .execute();
        log.info("Snapshot expiration completed for table: {}", table.name());
    }
}
