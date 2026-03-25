package org.example;

import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.example.service.MaintenanceService;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@Log4j2
@SpringBootApplication
@EnableConfigurationProperties
@RequiredArgsConstructor
public class IcebregMaintenanceApplication implements CommandLineRunner {

    private final MaintenanceService maintenanceService;

    public static void main(String[] args) {
        SpringApplication.run(IcebregMaintenanceApplication.class, args);
    }

    @Override
    public void run(String... args) {
        log.info("Icebreg Maintenance Application started");
        this.maintenanceService.runMaintenance();
        log.info("Icebreg Maintenance Application finished");
    }
}
