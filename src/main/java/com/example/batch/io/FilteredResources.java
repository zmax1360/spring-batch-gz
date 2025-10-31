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
        try {
            String normDir = normalizeDir(inputDir); // add file: if missing, fix slashes
            String pattern = normDir.endsWith("/") ? normDir + glob : normDir + "/" + glob;

            PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
            Resource[] all = resolver.getResources(pattern);

            Resource[] filtered = Arrays.stream(all)
                    .filter(Resource::isReadable)
                    .filter(r -> {
                        String n = r.getFilename() == null ? "" : r.getFilename();
                        return n.contains(mustContain);
                    })
                    .toArray(Resource[]::new);

            log.info("Resolved {} candidates under pattern: {}", all.length, pattern);
            for (Resource r : filtered) {
                log.info("Selected: {}", safeDesc(r));
            }
            log.info("Total selected after filename filter (contains='{}'): {}", mustContain, filtered.length);
            return filtered;

        } catch (IOException e) {
            throw new RuntimeException("Failed to list resources under inputDir=" + inputDir + " glob=" + glob, e);
        }
    }

    private static String normalizeDir(String inputDir) {
        if (inputDir == null || inputDir.isBlank()) return "file:.";
        // Already a resource location?
        if (inputDir.startsWith("file:") || inputDir.startsWith("classpath:")) return inputDir;

        // UNC like \\SERVER\Share\folder -> file:////SERVER/Share/folder
        if (inputDir.startsWith("\\\\")) {
            String noBack = inputDir.replace('\\', '/'); // //SERVER/Share/folder
            if (noBack.startsWith("//")) {
                return "file:" + noBack; // file:////SERVER/Share/folder
            }
            return "file:/" + noBack;
        }

        // Windows drive like C:\path -> file:///C:/path
        if (inputDir.matches("^[A-Za-z]:\\\\.*")) {
            String noBack = inputDir.replace('\\', '/'); // C:/path
            return "file:///" + noBack;
        }

        // Absolute *nix path like /mnt/network/path
        if (inputDir.startsWith("/")) return "file:" + inputDir;

        // Relative path
        return "file:" + (inputDir.startsWith("./") ? inputDir : "./" + inputDir);
    }

    private static String safeDesc(Resource r) {
        try {
            return r.getURL().toString();
        } catch (Exception e) {
            return String.valueOf(r);
        }
    }
}
