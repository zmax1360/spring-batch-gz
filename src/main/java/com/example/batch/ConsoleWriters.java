package com.example.batch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemWriter;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public final class ConsoleWriters {
  private static final Logger log = LoggerFactory.getLogger(ConsoleWriters.class);

  public static ItemWriter<LogRecord> byService() {
    Map<String, ItemWriter<LogRecord>> cache = new ConcurrentHashMap<>();
    return items -> {
      Map<String, List<LogRecord>> grouped =
          items.stream().collect(Collectors.groupingBy(r -> (r.serviceName()==null||r.serviceName().isBlank())?"other":r.serviceName()));
      for (var e : grouped.entrySet()) {
        cache.computeIfAbsent(e.getKey(), svc -> batch -> {
          for (var r : batch) log.info("[SERVICE={}] {}", svc, r.raw());
        }).write(e.getValue());
      }
    };
  }
}
