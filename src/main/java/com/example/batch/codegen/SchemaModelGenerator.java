package com.example.batch.codegen;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.*;
import java.nio.file.*;
import java.sql.*;
import java.util.*;

public class SchemaModelGenerator {
    private static final Logger log = LoggerFactory.getLogger(SchemaModelGenerator.class);

    public record Column(String name, int jdbcType, String typeName, int size, int scale, boolean nullable) {}

    public void generateRecordForTable(
            DataSource ds,
            String schema,
            String table,
            String basePackage,
            String className,
            Path outputDir
    ) throws Exception {
        List<Column> cols = readColumns(ds, schema, table);
        if (cols.isEmpty()) throw new IllegalStateException("No columns for " + schema + "." + table);

        String pkg = basePackage + "." + toPkg(schema) + "." + toPkg(table);
        Path pkgDir = outputDir.resolve(pkg.replace('.', '/'));
        Files.createDirectories(pkgDir);

        // build the record source
        String source = buildRecordSource(pkg, className, cols);

        Path javaFile = pkgDir.resolve(className + ".java");
        Files.writeString(javaFile, source);
        log.info("Wrote model {}.{} -> {}", pkg, className, javaFile.toAbsolutePath());
    }

    private List<Column> readColumns(DataSource ds, String schema, String table) throws Exception {
        try (Connection c = ds.getConnection()) {
            DatabaseMetaData md = c.getMetaData();
            try (ResultSet rs = md.getColumns(c.getCatalog(), schema, table, "%")) {
                List<Column> cols = new ArrayList<>();
                while (rs.next()) {
                    String col = rs.getString("COLUMN_NAME");
                    int jdbcType = rs.getInt("DATA_TYPE");
                    String typeName = rs.getString("TYPE_NAME");
                    int size = rs.getInt("COLUMN_SIZE");
                    int scale = rs.getInt("DECIMAL_DIGITS");
                    boolean nullable = "YES".equalsIgnoreCase(rs.getString("IS_NULLABLE"));
                    cols.add(new Column(col, jdbcType, typeName, size, scale, nullable));
                }
                return cols;
            }
        }
    }

    private String buildRecordSource(String pkg, String className, List<Column> cols) {
        StringBuilder sb = new StringBuilder();
        sb.append("package ").append(pkg).append(";\n\n");
        // find needed imports
        Set<String> imports = new LinkedHashSet<>();
        for (Column c : cols) {
            String jt = SqlTypeMapper.javaType(c.jdbcType(), c.typeName(), c.size(), c.scale());
            if (jt.contains(".")) imports.add(jt);
        }
        for (String imp : imports) sb.append("import ").append(imp).append(";\n");
        if (!imports.isEmpty()) sb.append("\n");

        sb.append("/** Generated from table columns. */\n");
        sb.append("public record ").append(className).append("(\n");
        for (int i = 0; i < cols.size(); i++) {
            Column c = cols.get(i);
            String jt = SqlTypeMapper.javaType(c.jdbcType(), c.typeName(), c.size(), c.scale());
            sb.append("  ").append(jt).append(" ").append(toField(c.name()));
            if (i < cols.size() - 1) sb.append(",\n");
        }
        sb.append("\n) {}\n");
        return sb.toString();
    }

    private String toField(String col) {
        String s = col.toLowerCase(Locale.ROOT);
        StringBuilder out = new StringBuilder();
        boolean up = false;
        for (char ch : s.toCharArray()) {
            if (ch == '_' || ch == ' ') { up = true; continue; }
            out.append(up ? Character.toUpperCase(ch) : ch);
            up = false;
        }
        return out.toString();
    }

    private String toPkg(String name) {
        return name.toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9]+", "");
    }
}

