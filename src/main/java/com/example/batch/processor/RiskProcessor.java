package com.example.batch.processor;

import com.example.batch.LogRecord;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;

@Component("riskProcessor")
@Slf4j
public class RiskProcessor implements ItemProcessor<LogRecord, LogRecord> {
    @Override
    public LogRecord process(LogRecord item) {
        // for now, just read and return
        log.info("RiskProcessor read line: " + item.raw());
        return item;
    }
}