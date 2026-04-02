package org.fusadora.dataflow.common;

/**
 * Bigquery schema constants.
 *
 * @author Parag Ghosh
 * @since 04/12/2025
 */
public class BQTableFieldSchemaConstants {
    public static final String TYPE_STRING = "STRING";
    public static final String TYPE_TIMESTAMP = "TIMESTAMP";
    public static final String TYPE_RECORD = "RECORD";
    public static final String TYPE_INTEGER = "INTEGER";
    public static final String TYPE_NUMERIC = "NUMERIC";
    public static final String TYPE_FLOAT = "FLOAT";
    public static final String TYPE_BOOLEAN = "BOOLEAN";
    public static final String TYPE_BYTES = "BYTES";
    public static final String TYPE_DATE = "DATE";
    public static final String TYPE_TIME = "TIME";
    public static final String TYPE_DATETIME = "DATETIME";

    public static final String MODE_REQUIRED = "REQUIRED";
    public static final String MODE_REPEATED = "REPEATED";
    public static final String MODE_OPTIONAL = "NULLABLE";

    public static final String INVALID_COLUMN_NAME_REGEX = "[^A-Za-z0-9€$]";
    public static final String CURRENCY_CODES_DOLLAR_REGEX = "[$]";
    public static final String CURRENCY_CODES_EURO_REGEX = "[€]";

    public static final String CURRENCY_CODE_DOLLAR_STRING = "DOLLAR";
    public static final String CURRENCY_CODE_EURO_STRING = "EURO";


    private BQTableFieldSchemaConstants() {
        throw new IllegalStateException("Utility class");
    }

}

