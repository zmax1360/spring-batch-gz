package com.example.batch.processor;

import com.example.batch.LogRecord;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;

@Component("defaultProcessor")
@Slf4j
public class DefaultProcessor implements ItemProcessor<LogRecord, LogRecord> {
    @Override
    public LogRecord process(LogRecord item) {
        log.info("DefaultProcessor read line: " + item.raw());
        return item;
    }
}