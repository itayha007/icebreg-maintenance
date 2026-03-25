package org.example.maintenance.impl;

import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.iceberg.Table;
import org.apache.iceberg.spark.actions.SparkActions;
import org.apache.spark.sql.SparkSession;
import org.example.maintenance.IcebergMaintainer;
import org.springframework.stereotype.Component;

@Log4j2
@Component
@RequiredArgsConstructor
public class DeleteOrphanFilesMaintainer implements IcebergMaintainer {

    private final SparkSession spark;

    @Override
    public void maintain(Table table) {
        log.info("Deleting orphan files on table: {}", table.name());
        SparkActions.get(this.spark).deleteOrphanFiles(table).execute();
    }
}
