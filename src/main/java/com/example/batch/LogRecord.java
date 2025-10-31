package com.example.batch;

public record LogRecord(
    String raw,
    String timestamp,
    String serviceName,
    String reactTime,
    String destinationSeq,
    String seq,
    String entity
) {}
