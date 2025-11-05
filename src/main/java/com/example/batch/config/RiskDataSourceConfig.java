package com.example.batch.config;

import com.zaxxer.hikari.HikariDataSource;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;

@Configuration(proxyBeanMethods = false)
public class RiskDataSourceConfig {

    @Bean
    public DataSource riskSourceDataSource(
            @Value("${app.riskSource.url}") String url,
            @Value("${app.riskSource.username}") String user,
            @Value("${app.riskSource.password}") String pass) {
        var ds = new HikariDataSource();
        ds.setJdbcUrl(url); ds.setUsername(user); ds.setPassword(pass);
        ds.setPoolName("risk-source");
        return ds;
    }

    @Bean
    public DataSource riskDestDataSource(
            @Value("${app.riskDest.url}") String url,
            @Value("${app.riskDest.username}") String user,
            @Value("${app.riskDest.password}") String pass) {
        var ds = new HikariDataSource();
        ds.setJdbcUrl(url); ds.setUsername(user); ds.setPassword(pass);
        ds.setPoolName("risk-dest");
        return ds;
    }

    @Bean public JdbcTemplate riskSourceJdbcTemplate(DataSource riskSourceDataSource) {
        return new JdbcTemplate(riskSourceDataSource);
    }

    @Bean public JdbcTemplate riskDestJdbcTemplate(DataSource riskDestDataSource) {
        return new JdbcTemplate(riskDestDataSource);
    }
}