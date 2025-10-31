// com/example/batch/ConsoleWriters.java
package com.example.batch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public final class ConsoleWriters {
  private static final Logger log = LoggerFactory.getLogger(ConsoleWriters.class);

  public static ItemWriter<LogRecord> byService() {
    return (Chunk<? extends LogRecord> items) -> {
      Map<String, List<LogRecord>> grouped = items.getItems().stream()
              .collect(Collectors.groupingBy(r -> sanitize(r.serviceName())));

      for (var e : grouped.entrySet()) {
        String svc = e.getKey();
        try {
          MDC.put("service", svc); // drives per-service file selection
          for (LogRecord r : e.getValue()) {
            log.info(r.toLogLine());
          }
        } finally {
          MDC.remove("service");
        }
      }
    };
  }

  // keep filenames safe: letters, digits, dash, underscore
  static String sanitize(String s) {
    if (s == null || s.isBlank()) return "unknown";
    String cleaned = s.trim().toLowerCase().replaceAll("[^a-z0-9-_]+", "_");
    return cleaned.isBlank() ? "unknown" : cleaned;
  }
}
