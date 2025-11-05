package com.example.batch.codegen;


import java.sql.Types;
import java.util.Map;

public final class SqlTypeMapper {
    private SqlTypeMapper() {}

    public static String javaType(int jdbcType, String typeName, int size, int scale) {
        // common cases first
        return switch (jdbcType) {
            case Types.VARCHAR, Types.CHAR, Types.LONGVARCHAR, Types.NVARCHAR, Types.NCHAR, Types.LONGNVARCHAR -> "String";
            case Types.BOOLEAN, Types.BIT -> "Boolean";
            case Types.TINYINT, Types.SMALLINT, Types.INTEGER -> "Integer";
            case Types.BIGINT -> "Long";
            case Types.DECIMAL, Types.NUMERIC -> "java.math.BigDecimal";
            case Types.REAL, Types.FLOAT -> "Float";
            case Types.DOUBLE -> "Double";
            case Types.DATE -> "java.time.LocalDate";
            case Types.TIME -> "java.time.LocalTime";
            case Types.TIMESTAMP, Types.TIMESTAMP_WITH_TIMEZONE -> "java.time.OffsetDateTime";
            case Types.BINARY, Types.VARBINARY, Types.LONGVARBINARY, Types.BLOB, Types.OTHER -> "byte[]";
            default -> "String"; // safe fallback
        };
    }
}

