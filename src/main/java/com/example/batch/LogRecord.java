// com/example/batch/LogRecord.java
package com.example.batch;

import java.util.List;

public record LogRecord(String raw, List<String> fields) {
    public String serviceName() {
        if (fields == null || fields.size() <= 1) return "unknown";
        String s = fields.get(1);
        return (s == null || s.isBlank()) ? "unknown" : s.trim();
    }

    // Optional: one place to define how the line is written to log files
    public String toLogLine() { return raw; }
}
