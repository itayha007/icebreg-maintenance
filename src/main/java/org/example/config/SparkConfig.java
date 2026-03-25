package org.example.config;

import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SparkConfig {

    @Value("${spark.master:local[*]}")
    private String sparkMaster;

    @Value("${spark.app-name:icebreg-maintenance}")
    private String appName;

    private final IcebergProperties icebergProperties;

    public SparkConfig(IcebergProperties icebergProperties) {
        this.icebergProperties = icebergProperties;
    }

    @Bean
    public SparkSession sparkSession() {
        String catalogName = this.icebergProperties.getCatalog().getName();
        String catalogType = this.icebergProperties.getCatalog().getType();
        String warehouse  = this.icebergProperties.getCatalog().getWarehouse();
        String region     = this.icebergProperties.getCatalog().getGlue().getRegion();

        SparkSession.Builder builder = SparkSession.builder()
                .master(this.sparkMaster)
                .appName(this.appName)
                .config("spark.sql.extensions",
                        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                .config("spark.sql.catalog." + catalogName,
                        "org.apache.iceberg.spark.SparkCatalog")
                .config("spark.sql.catalog." + catalogName + ".warehouse", warehouse);

        if ("glue".equalsIgnoreCase(catalogType)) {
            builder
                .config("spark.sql.catalog." + catalogName + ".catalog-impl",
                        "org.apache.iceberg.aws.glue.GlueCatalog")
                .config("spark.sql.catalog." + catalogName + ".io-impl",
                        "org.apache.iceberg.aws.s3.S3FileIO")
                .config("spark.sql.catalog." + catalogName + ".glue.region", region);
        } else {
            builder.config("spark.sql.catalog." + catalogName + ".type", catalogType);
        }

        return builder.getOrCreate();
    }
}
