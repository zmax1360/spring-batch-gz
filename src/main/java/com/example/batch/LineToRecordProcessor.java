package com.example.batch;

import org.springframework.batch.item.ItemProcessor;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class LineToRecordProcessor implements ItemProcessor<String, LogRecord> {
  @Override
  public LogRecord process(String line) {
    if (line == null || line.isBlank()) return null;
    List<String> fields = Arrays.stream(line.split(",", -1))
            .map(String::trim)
            .collect(Collectors.toList());
    return new LogRecord(line, fields);
  }
}