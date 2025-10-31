package com.example.batch.io;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;

import java.io.IOException;
import java.util.Arrays;

public final class FilteredResources {
    private static final Logger log = LoggerFactory.getLogger(FilteredResources.class);
    private FilteredResources() {}

    public static Resource[] gzWithName(String inputDir, String glob, String mustContain) {
        String normDir = normalizeDir(inputDir);
        String pattern = normDir.endsWith("/") ? normDir + glob : normDir + "/" + glob;

        log.info("Input dir (raw)  : {}", inputDir);
        log.info("Input dir (norm) : {}", normDir);
        log.info("Glob pattern     : {}", pattern);

        try {
            PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
            Resource[] candidates = resolver.getResources(pattern);

            log.info("Found {} candidate resource(s) before filename filter:", candidates.length);
            for (Resource r : candidates) {
                log.info("  - {}", safeUrl(r));
            }

            Resource[] selected = Arrays.stream(candidates)
                    .filter(Resource::isReadable)
                    .filter(r -> {
                        String n = r.getFilename() == null ? "" : r.getFilename();
                        return n.contains(mustContain);
                    })
                    .toArray(Resource[]::new);

            log.info("Selected {} resource(s) after filter contains='{}':", selected.length, mustContain);
            for (Resource r : selected) {
                log.info("  ✓ {}", safeUrl(r));
            }

            return selected;
        } catch (IOException e) {
            throw new RuntimeException("Failed to list resources. inputDir=" + inputDir + " glob=" + glob, e);
        }
    }

    private static String normalizeDir(String inputDir) {
        if (inputDir == null || inputDir.isBlank()) return "file:.";
        if (inputDir.startsWith("file:") || inputDir.startsWith("classpath:")) return inputDir;

        // UNC like \\SERVER\Share\folder -> file:////SERVER/Share/folder
        if (inputDir.startsWith("\\\\")) {
            String noBack = inputDir.replace('\\', '/'); // //SERVER/Share/folder
            return noBack.startsWith("//") ? "file:" + noBack : "file:/" + noBack;
        }
        // Windows drive C:\path -> file:///C:/path
        if (inputDir.matches("^[A-Za-z]:\\\\.*")) {
            String noBack = inputDir.replace('\\', '/'); // C:/path
            return "file:///" + noBack;
        }
        // *nix absolute
        if (inputDir.startsWith("/")) return "file:" + inputDir;

        // relative
        return "file:" + (inputDir.startsWith("./") ? inputDir : "./" + inputDir);
    }

    private static String safeUrl(Resource r) {
        try { return r.getURL().toString(); } catch (Exception e) { return r.toString(); }
    }
}
