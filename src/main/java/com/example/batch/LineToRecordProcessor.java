package com.example.batch;

import org.springframework.batch.item.ItemProcessor;

public class LineToRecordProcessor implements ItemProcessor<String, LogRecord> {
  @Override public LogRecord process(String line) {
    if (line == null) return null;
    String[] p = line.split(",", -1);
    String ts  = p.length > 0 ? p[0].trim() : "";
    String svc = p.length > 1 ? p[1].trim() : "";
    String rt  = p.length > 2 ? p[2].trim() : "";
    String dSq = p.length > 3 ? p[3].trim() : "";
    String sq  = p.length > 4 ? p[4].trim() : "";
    String ent = p.length > 5 ? p[5].trim() : "";
    return new LogRecord(line, ts, svc, rt, dSq, sq, ent);
  }
}
