package org.example.config;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;
import org.springframework.context.annotation.Bean;

import java.util.HashMap;
import java.util.Map;

@org.springframework.context.annotation.Configuration
public class IcebergCatalogConfig {

    private final IcebergProperties properties;

    public IcebergCatalogConfig(IcebergProperties properties) {
        this.properties = properties;
    }

    @Bean
    public Catalog icebergCatalog() {
        IcebergProperties.CatalogProperties catalogProps = this.properties.getCatalog();
        String type = catalogProps.getType();
        String warehouse = catalogProps.getWarehouse();

        Map<String, String> catalogConfig = new HashMap<>();
        catalogConfig.put("warehouse", warehouse);

        String catalogImpl;
        if ("glue".equalsIgnoreCase(type)) {
            catalogImpl = "org.apache.iceberg.aws.glue.GlueCatalog";
            catalogConfig.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
            catalogConfig.put("glue.region", catalogProps.getGlue().getRegion());
            if (catalogProps.getGlue().getAccountId() != null) {
                catalogConfig.put("glue.account-id", catalogProps.getGlue().getAccountId());
            }
        } else if ("rest".equalsIgnoreCase(type)) {
            catalogImpl = "org.apache.iceberg.rest.RESTCatalog";
        } else if ("hive".equalsIgnoreCase(type)) {
            catalogImpl = "org.apache.iceberg.hive.HiveCatalog";
        } else {
            // default: hadoop
            catalogImpl = "org.apache.iceberg.hadoop.HadoopCatalog";
        }

        return CatalogUtil.loadCatalog(catalogImpl, catalogProps.getName(), catalogConfig, new Configuration());
    }
}
