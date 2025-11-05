package com.example.batch.codegen;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.*;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.nio.file.Path;

@Configuration
@Profile("codegen")
public class CodegenRunner {
    private static final Logger log = LoggerFactory.getLogger(CodegenRunner.class);

    @Bean
    ApplicationRunner generateModels(
            @Value("${app.modelgen.basePackage}") String basePackage,
            @Value("${app.modelgen.outputDir}") String outputDir,

            @Value("${app.modelgen.risk.source.schema}") String riskSrcSchema,
            @Value("${app.modelgen.risk.source.table}")  String riskSrcTable,
            @Value("${app.modelgen.risk.source.class}")  String riskSrcClass,

            @Value("${app.modelgen.risk.dest.schema}") String riskDstSchema,
            @Value("${app.modelgen.risk.dest.table}")  String riskDstTable,
            @Value("${app.modelgen.risk.dest.class}")  String riskDstClass,

            @Qualifier("sourceDataSource") DataSource pg,
            @Qualifier("destDataSource")   DataSource ora
    ) {
        return args -> {
            var gen = new SchemaModelGenerator();
            Path out = Path.of(outputDir);

            log.info("Generating models into {}", out.toAbsolutePath());

            gen.generateRecordForTable(pg,  riskSrcSchema, riskSrcTable, basePackage, riskSrcClass, out);
            gen.generateRecordForTable(ora, riskDstSchema, riskDstTable, basePackage, riskDstClass, out);

            log.info("Model codegen done.");
        };
    }
}
//mvn -q -DskipTests package
//java -Dspring.profiles.active=codegen \
//        -jar target/spring-batch-gz-0.0.1-SNAPSHOT.jar
