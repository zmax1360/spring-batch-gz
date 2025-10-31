package com.example.batch.io;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.ResourceAware;
import org.springframework.batch.item.support.AbstractItemStreamItemReader;
import org.springframework.core.io.Resource;
import org.springframework.lang.NonNull;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPInputStream;

/**
 * A restartable line reader for a single .gz resource.
 * This class is Resource-aware so it can be used as the delegate of MultiResourceItemReader.
 */
public class GzipLineItemReader extends AbstractItemStreamItemReader<String> implements ResourceAware {

  private Resource resource;
  private BufferedReader br;
  private long lineIndex = 0L;
  private String checkpointKey;

  public GzipLineItemReader() { setName("gzipLineReader"); }

  @Override
  public void setResource(@NonNull Resource resource) {
    this.resource = resource;
    this.checkpointKey = resource.getFilename() + ".line";
  }

  @Override
  public void open(ExecutionContext executionContext) throws ItemStreamException {
    try {
      long saved = executionContext.containsKey(checkpointKey) ? executionContext.getLong(checkpointKey) : 0L;
      this.lineIndex = saved;
      var is = new GZIPInputStream(resource.getInputStream());
      this.br = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));
      for (long i = 0; i < lineIndex; i++) br.readLine(); // fast-forward
    } catch (IOException e) { throw new UncheckedIOException(e); }
  }

  @Override
  public void update(ExecutionContext executionContext) throws ItemStreamException {
    executionContext.putLong(checkpointKey, lineIndex);
  }

  @Override
  public void close() throws ItemStreamException {
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
