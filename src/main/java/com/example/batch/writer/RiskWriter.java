package com.example.batch.writer;

import com.example.batch.LogRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.example.batch.LogRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.interceptor.TransactionAspectSupport;

import java.util.Map;

@Component("riskWriter")
public class RiskWriter implements ItemWriter<LogRecord> {

    private static final Logger log = LoggerFactory.getLogger(RiskWriter.class);

    private final JdbcTemplate riskSourceJdbcTemplate;
    private final JdbcTemplate riskDestJdbcTemplate;
    private final String selectSql;
    private final String insertSql;
    private final boolean dryRun;

    public RiskWriter(JdbcTemplate riskSourceJdbcTemplate,
                      JdbcTemplate riskDestJdbcTemplate,
                     @Value("${app.risk.selectSql}") String selectSql,
                      @Value("${app.risk.insertSql}") String insertSql,
                      @Value("${app.risk.dryRun:false}") boolean dryRun) {
        this.riskSourceJdbcTemplate = riskSourceJdbcTemplate;
        this.riskDestJdbcTemplate = riskDestJdbcTemplate;
        this.selectSql = selectSql;
        this.insertSql = insertSql;
        this.dryRun = dryRun;
    }

    @Override
    public void write(Chunk<? extends LogRecord> items) {
        MDC.put("service", "risk");  // goes to logs/risk.log as well as main
        try {
            for (LogRecord r : items) {
                // 1) extract id (fields[4])
                String id = (r.fields().size() > 4) ? r.fields().get(4) : null;
                if (id == null || id.isBlank()) {
                    log.warn("RISK skip: missing id in line: {}", r.raw());
                    continue;
                }

                // 2) read from source DB
                Map<String, Object> src;
                try {
                    src = riskSourceJdbcTemplate.queryForMap(selectSql, id);
                    log.info("RISK source read id={} row={}", id, src);
                } catch (org.springframework.dao.EmptyResultDataAccessException e) {
                    log.warn("RISK source read id={} -> NO ROW", id);
                    continue;
                }

                // 3) insert into destination DB (map your params)
                Object colA = src.get("col_a");
                Object colB = src.get("col_b");
                int wrote = riskDestJdbcTemplate.update(insertSql, id, colA, colB);
                log.info("RISK dest write id={} rowsAffected={}", id, wrote);
            }

            // 4) soft-run: mark the chunk transaction for rollback
            if (dryRun) {
                TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
                log.info("RISK dryRun=true -> chunk marked rollbackOnly (no changes committed).");
            }

        } finally {
            MDC.remove("service");
        }
    }
}