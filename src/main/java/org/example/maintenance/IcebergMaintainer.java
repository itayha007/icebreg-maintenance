package org.example.maintenance;

import org.apache.iceberg.Table;

public interface IcebergMaintainer {

    void maintain(Table table);
}
