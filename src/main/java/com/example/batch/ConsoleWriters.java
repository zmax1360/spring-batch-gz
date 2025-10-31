package com.example.batch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.Chunk;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public final class ConsoleWriters {
  private static final Logger log = LoggerFactory.getLogger(ConsoleWriters.class);

  public static ItemWriter<LogRecord> byService() {
    return (Chunk<? extends LogRecord> items) -> {
      Map<String, List<LogRecord>> grouped = items.getItems().stream()
              .map(r -> (LogRecord) r)
              .collect(Collectors.groupingBy(r ->
                      (r.serviceName() == null || r.serviceName().isBlank()) ? "other" : r.serviceName()));

      grouped.forEach((svc, list) -> {
        for (LogRecord r : list) {
          log.info("[SERVICE={}] {}", svc, r.raw());
        }
      });
    };
  }
}
