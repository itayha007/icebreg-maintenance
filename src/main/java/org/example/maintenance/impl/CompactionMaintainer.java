package org.example.maintenance.impl;

import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.iceberg.Table;
import org.apache.iceberg.spark.actions.SparkActions;
import org.apache.spark.sql.SparkSession;
import org.example.maintenance.IcebergMaintainer;

@Log4j2
@RequiredArgsConstructor
public class CompactionMaintainer implements IcebergMaintainer {

    private final SparkSession spark;

    @Override
    public void maintain(Table table) {
        log.info("Running compaction (rewrite data files) on table: {}", table.name());
        SparkActions.get(spark)
                .rewriteDataFiles(table)
                .execute();
        log.info("Compaction completed for table: {}", table.name());
    }
}
