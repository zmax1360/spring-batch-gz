// was: extends AbstractItemStreamItemReader<String> implements ResourceAware
package com.example.batch.io;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.file.ResourceAwareItemReaderItemStream;
import org.springframework.core.io.Resource;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPInputStream;

public class GzipLineItemReader implements ResourceAwareItemReaderItemStream<String> {

  private Resource resource;
  private BufferedReader br;
  private long lineIndex = 0L;
  private String checkpointKey;

  @Override
  public void setResource(Resource resource) {
    this.resource = resource;
    this.checkpointKey = (resource.getFilename() == null ? "unknown" : resource.getFilename()) + ".line";
    try {
      org.slf4j.LoggerFactory.getLogger(getClass())
              .info("Opening resource: {}", resource.getURL());
    } catch (Exception ignore) {
    }
  }

  @Override
  public void open(ExecutionContext ec) {
    try {
      this.lineIndex = ec.containsKey(checkpointKey) ? ec.getLong(checkpointKey) : 0L;
      var is = new GZIPInputStream(resource.getInputStream());
      this.br = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));
      for (long i = 0; i < lineIndex; i++) br.readLine(); // fast-forward to checkpoint
    } catch (IOException e) { throw new UncheckedIOException(e); }
    org.slf4j.LoggerFactory.getLogger(getClass())
            .info("Starting at line={} for resource={}", lineIndex, checkpointKey);
  }

  @Override
  public void update(ExecutionContext ec) {
    ec.putLong(checkpointKey, lineIndex);
  }

  @Override
  public void close() {
    try { if (br != null) br.close(); } catch (IOException ignored) {}
  }

  @Override
  public String read() {
    try {
      String line = br.readLine();
      if (line != null) lineIndex++;
      return line;
    } catch (IOException e) { throw new UncheckedIOException(e); }
  }
}
