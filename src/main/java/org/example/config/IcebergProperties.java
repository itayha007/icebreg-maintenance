package org.example.config;

import lombok.Data;
import org.example.maintenance.MaintenanceType;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.List;

@Data
@Component
@ConfigurationProperties(prefix = "iceberg")
public class IcebergProperties {

    private List<String> databases;
    private CatalogProperties catalog = new CatalogProperties();
    private MaintenanceProperties maintenance = new MaintenanceProperties();

    @Data
    public static class CatalogProperties {
        private String name = "glue_catalog";
        private String type = "glue";
        private String warehouse;
        private GlueProperties glue = new GlueProperties();

        @Data
        public static class GlueProperties {
            private String accountId;
            private String region = "us-east-1";
        }
    }

    @Data
    public static class MaintenanceProperties {
        private List<MaintenanceType> enabledTypes = List.of(MaintenanceType.values());
    }
}
