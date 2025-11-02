package com.example.batch.writer;

import com.example.batch.LogRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;
import org.springframework.stereotype.Component;

@Component("riskWriter")
public class RiskWriter implements ItemWriter<LogRecord> {
    private static final Logger log = LoggerFactory.getLogger(RiskWriter.class);
    @Override public void write(Chunk<? extends LogRecord> items) {
        try {
            MDC.put("service", "riskWriter");
            for (var r : items) log.info(r.raw());
        } finally { MDC.remove("service"); }
    }
}