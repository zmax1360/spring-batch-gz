package com.example.batch.io;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.file.ResourceAwareItemReaderItemStream;
import org.springframework.core.io.Resource;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPInputStream;

/**
 * Reads a .gz file line-by-line and remembers its position for checkpointing.
 * Designed for use as a delegate of MultiResourceItemReader<String>.
 */
public class GzipLineItemReader implements ResourceAwareItemReaderItemStream<String> {

  private static final Logger log = LoggerFactory.getLogger(GzipLineItemReader.class);

  private Resource resource;
  private BufferedReader br;
  private long lineIndex = 0L;
  private String checkpointKey;

  @Override
  public void setResource(Resource resource) {
    this.resource = resource;
    this.checkpointKey =
            (resource.getFilename() == null ? "unknown" : resource.getFilename()) + ".line";
    try {
      log.info("setResource → {}", resource.getURL());
    } catch (Exception e) {
      log.warn("Failed to resolve resource URL: {}", e.getMessage());
    }
  }

  @Override
  public void open(ExecutionContext ec) {
    if (resource == null) {
      throw new IllegalStateException("Resource not set before open()");
    }
    try {
      this.lineIndex = ec.containsKey(checkpointKey) ? ec.getLong(checkpointKey) : 0L;
      var gz = new GZIPInputStream(resource.getInputStream());
      this.br = new BufferedReader(new InputStreamReader(gz, StandardCharsets.UTF_8));
      for (long i = 0; i < lineIndex; i++) br.readLine(); // fast-forward to checkpoint
      log.info("open → start at line={} for {}", lineIndex, checkpointKey);
    } catch (IOException e) {
      throw new UncheckedIOException("Error opening gz resource: " + resource, e);
    }
  }

  @Override
  public String read() {
    if (br == null) {
      throw new IllegalStateException("read() called before open(); br is null for " + checkpointKey);
    }
    try {
      String line = br.readLine();
      if (line != null) {
        lineIndex++;
        if ((lineIndex % 10000) == 0) {
          log.info("progress → {} lines read from {}", lineIndex, checkpointKey);
        }
      }
      return line; // returning null signals EOF to Spring Batch
    } catch (IOException e) {
      throw new UncheckedIOException("Error reading gz resource: " + resource, e);
    }
  }

  @Override
  public void update(ExecutionContext ec) {
    ec.putLong(checkpointKey, lineIndex);
  }

  @Override
  public void close() {
    try {
      if (br != null) {
        br.close();
      }
    } catch (IOException e) {
      log.warn("Error closing {}: {}", checkpointKey, e.toString());
    } finally {
      br = null;
      resource = null;
      lineIndex = 0L;
      checkpointKey = null;
    }
  }
}
