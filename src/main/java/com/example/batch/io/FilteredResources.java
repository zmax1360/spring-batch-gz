package com.example.batch.io;

import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;

import java.io.IOException;
import java.util.Arrays;

public final class FilteredResources {
  private FilteredResources() {}

  public static Resource[] gzWithName(String inputDir, String glob, String mustContain) {
    try {
      var resolver = new PathMatchingResourcePatternResolver();
      var pattern = inputDir.endsWith("/") ? inputDir + glob : inputDir + "/" + glob;
      Resource[] all = resolver.getResources(pattern);
      return Arrays.stream(all)
          .filter(Resource::isReadable)
          .filter(r -> {
            var n = r.getFilename() == null ? "" : r.getFilename();
            return n.contains(mustContain);
          })
          .toArray(Resource[]::new);
    } catch (IOException e) { throw new RuntimeException(e); }
  }
}
