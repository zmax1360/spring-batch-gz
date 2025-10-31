package com.example.batch;


import java.util.List;

public record LogRecord(String raw, List<String> fields) {
    public String serviceName() {
        return fields.size() > 1 ? fields.get(1) : "unknown";
    }
}
