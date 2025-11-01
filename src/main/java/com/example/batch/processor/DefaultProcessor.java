package com.example.batch.processor;

import com.example.batch.LogRecord;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;

@Component("defaultProcessor")
public class DefaultProcessor implements ItemProcessor<LogRecord, LogRecord> {
    @Override
    public LogRecord process(LogRecord item) {
        System.out.println("DefaultProcessor read line: " + item.raw());
        return item;
    }
}