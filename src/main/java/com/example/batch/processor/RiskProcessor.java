package com.example.batch.processor;

import com.example.batch.LogRecord;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;

@Component("riskProcessor")
public class RiskProcessor implements ItemProcessor<LogRecord, LogRecord> {
    @Override
    public LogRecord process(LogRecord item) {
        // for now, just read and return
        System.out.println("RiskProcessor read line: " + item.raw());
        return item;
    }
}